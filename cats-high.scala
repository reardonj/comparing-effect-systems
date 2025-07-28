package ma.chinespirit.crawldown

import cats.effect.{IO, Ref}
import cats.effect.std.Queue
import cats.syntax.all.*

import fs2.Stream

import sttp.model.Uri
import fs2.concurrent.Channel
import cats.effect.std.Semaphore

final class CatsScraperHighLevel(
    fetch: Fetch[IO],
    store: Store[IO],
    root: Uri,
    selector: Option[String],
    maxDepth: Int,
    parallelism: Int = 8
):

  def start: IO[Unit] =
    (Semaphore[IO](parallelism), Ref.of[IO, Set[Uri]](Set.empty)).flatMapN { case (semaphore, visited) =>
      def enqueue(target: Scrape): IO[Unit] =
        if target.depth >= maxDepth then IO.unit
        else
          visited
            .getAndUpdate(visited => visited + target.uri)
            .flatMap(visitedBefore => IO.whenA(!visitedBefore.contains(target.uri))(crawl(target.uri, target.depth)))

      def crawl(uri: Uri, depth: Int): IO[Unit] =
        semaphore.permit
          .use { _ =>
            for
              content <- fetch.fetch(uri)
              (links, markdown) <- IO.fromEither(MdConverter.convertAndExtractLinks(content, uri, selector))
              pushFrontier = links.map(Scrape(_, depth + 1)).parTraverse_(enqueue(_))
              persist = store.store(Names.toFilename(uri, root), markdown)
            yield persist.parProductR(pushFrontier)
          }
          .flatten
          .void

      enqueue(Scrape(root, 0))
    }
