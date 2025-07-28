package ma.chinespirit.crawldown

import cats.effect.{IO, Ref}
import cats.effect.std.Queue
import cats.syntax.all.*

import fs2.Stream

import sttp.model.Uri
import fs2.concurrent.Channel

final class CatsScraperHighLevel(
    fetch: Fetch[IO],
    store: Store[IO],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):

  def start: IO[Unit] =
    (Channel.unbounded[IO, Scrape], Ref.of[IO, Set[Uri]](Set.empty)).flatMapN { case (channel, visited) =>
      def enqueue(target: Scrape): IO[Int] =
        if target.depth >= maxDepth then IO.pure(0)
        else
          visited
            .getAndUpdate(_ + target.uri)
            .flatMap(visitedBefore => if visitedBefore.contains(target.uri) then IO.pure(0) else channel.send(target) >> IO.pure(1))

      def crawl(uri: Uri, depth: Int): IO[Int] =
        for
          content <- fetch.fetch(uri)
          (links, markdown) <- IO.fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
          pushFrontier = links.map(Scrape(_, depth + 1)).foldMapM(enqueue)
          persist = store.store(Names.toFilename(uri, root), markdown)
          enqueued <- persist.parProductR(pushFrontier)
        yield enqueued

      enqueue(Scrape(root, 0)) >>
        channel.stream
          .parEvalMap(maxConcurrent = parallelism) { case Scrape(uri, depth) => crawl(uri, depth) }
          .scan(1) { _ + _ - 1 }
          .takeWhile(_ > 0)
          .compile
          .drain
    }
