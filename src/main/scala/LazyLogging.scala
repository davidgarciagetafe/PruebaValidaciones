import org.slf4j.{Logger, LoggerFactory}

/**
 * Defines `logger` as a lazy value initialized with an underlying `org.slf4j.Logger`
 * named according to the class into which this trait is mixed.
 */
trait LazyLogging {

  @transient
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

}
