import scala.io.Source
import scala.util.Try
import scala.collection.parallel.CollectionConverters._
import java.io.PrintWriter
import scala.util.Using

object Main:

  // ============================================================
  // Data Models
  // ============================================================
  final case class AccidentRecord(
      year: Int,
      province: String,
      cause: String,
      weather: String,
      deaths: Int,
      seriousInjuries: Int,
      minorInjuries: Int,
      totalInjuries: Int,
      latitude: Double,
      longitude: Double
  )

  final case class EnrichedRecord(
      year: Int,
      province: String,
      cause: String,
      weather: String,
      deaths: Int,
      seriousInjuries: Int,
      minorInjuries: Int,
      totalInjuries: Int,
      severityScore: Int,
      riskLevel: String
  )

  final case class SummaryResult(
      totalAccidents: Int,
      totalDeaths: Int,
      totalSeriousInjuries: Int,
      totalMinorInjuries: Int,
      totalAllInjuries: Int,
      highRiskCases: Int,
      top5Provinces: List[(String, Int)]
  )

  final case class CleanReport(
      inputRows: Int,
      outputRows: Int,
      invalidRows: Int,
      sampleOutput: Option[AccidentRecord],
      seqTimeMs: Double,
      parTimeMs: Double,
      sameResult: Boolean
  )

  final case class TransformReport(
      inputRows: Int,
      outputRows: Int,
      removedZeroCoordinates: Int,
      sampleInput: Option[AccidentRecord],
      sampleOutput: Option[EnrichedRecord],
      seqTimeMs: Double,
      parTimeMs: Double,
      sameResult: Boolean
  )

  final case class SummaryReport(
      inputRows: Int,
      summary: SummaryResult,
      seqTimeMs: Double,
      parTimeMs: Double,
      sameResult: Boolean
  )

  final case class PipelineOutput(
      cleanData: Vector[AccidentRecord],
      transformedData: Vector[EnrichedRecord],
      summary: SummaryResult,
      cleanReport: CleanReport,
      transformReport: TransformReport,
      summaryReport: SummaryReport
  )

  // ============================================================
  // PURE FUNCTIONS SECTION
  // ============================================================

  // ---------------------------
  // CSV Parser (Pure)
  // ---------------------------
  def parseCsvLine(line: String): List[String] =
    @annotation.tailrec
    def loop(
        chars: List[Char],
        current: String,
        inQuotes: Boolean,
        acc: List[String]
    ): List[String] =
      chars match
        case Nil => (acc :+ current).map(_.trim)
        case '"' :: '"' :: tail if inQuotes =>
          loop(tail, current + "\"", inQuotes, acc)
        case '"' :: tail =>
          loop(tail, current, !inQuotes, acc)
        case ',' :: tail if !inQuotes =>
          loop(tail, "", inQuotes, acc :+ current)
        case ch :: tail =>
          loop(tail, current + ch, inQuotes, acc)

    loop(line.toList, "", false, Nil)

  // ---------------------------
  // Safe Parsing (Pure)
  // ---------------------------
  def safeInt(s: String): Option[Int] =
    Try(s.trim.replace(",", "").toInt).toOption

  def safeDouble(s: String): Option[Double] =
    Try(s.trim.replace(",", "").toDouble).toOption

  def getField(
      fields: List[String],
      headerMap: Map[String, Int],
      name: String
  ): Option[String] =
    headerMap.get(name).flatMap(fields.lift)

  // ---------------------------
  // Parse + Clean Validation (Pure)
  // ---------------------------
  def parseRecord(
      line: String,
      headerMap: Map[String, Int]
  ): Either[String, AccidentRecord] =
    val fields = parseCsvLine(line)

    val result = for
      year <- getField(fields, headerMap, "ปีที่เกิดเหตุ").flatMap(safeInt)
      province <- getField(fields, headerMap, "จังหวัด").map(_.trim).filter(_.nonEmpty)
      cause <- getField(fields, headerMap, "มูลเหตุสันนิษฐาน").map(_.trim).filter(_.nonEmpty)
      weather <- getField(fields, headerMap, "สภาพอากาศ").map(_.trim).filter(_.nonEmpty)
      deaths <- getField(fields, headerMap, "ผู้เสียชีวิต").flatMap(safeInt)
      serious <- getField(fields, headerMap, "ผู้บาดเจ็บสาหัส").flatMap(safeInt)
      minor <- getField(fields, headerMap, "ผู้บาดเจ็บเล็กน้อย").flatMap(safeInt)
      total <- getField(fields, headerMap, "รวมจำนวนผู้บาดเจ็บ").flatMap(safeInt)
      lat <- getField(fields, headerMap, "LATITUDE").flatMap(safeDouble)
      lon <- getField(fields, headerMap, "LONGITUDE").flatMap(safeDouble)
    yield AccidentRecord(
      year = year,
      province = province,
      cause = cause,
      weather = weather,
      deaths = deaths,
      seriousInjuries = serious,
      minorInjuries = minor,
      totalInjuries = total,
      latitude = lat,
      longitude = lon
    )

    result.toRight(s"Bad row: $line")

  def cleanDataSeq(
      lines: Vector[String],
      headerMap: Map[String, Int]
  ): Vector[AccidentRecord] =
    lines.flatMap(line => parseRecord(line, headerMap).toOption)

  def cleanDataPar(
      lines: Vector[String],
      headerMap: Map[String, Int]
  ): Vector[AccidentRecord] =
    lines.par.flatMap(line => parseRecord(line, headerMap).toOption).toVector

  def countInvalidRows(
      lines: Vector[String],
      headerMap: Map[String, Int]
  ): Int =
    lines.count(line => parseRecord(line, headerMap).isLeft)

  // ---------------------------
  // Transform (Pure)
  // ---------------------------
  def enrichRecord(r: AccidentRecord): EnrichedRecord =
    val severityScore =
      (r.deaths * 5) + (r.seriousInjuries * 3) + r.minorInjuries

    val riskLevel =
      if severityScore >= 10 then "High"
      else if severityScore >= 4 then "Medium"
      else "Low"

    EnrichedRecord(
      year = r.year,
      province = r.province,
      cause = r.cause,
      weather = r.weather,
      deaths = r.deaths,
      seriousInjuries = r.seriousInjuries,
      minorInjuries = r.minorInjuries,
      totalInjuries = r.totalInjuries,
      severityScore = severityScore,
      riskLevel = riskLevel
    )

  def transformSeq(data: Vector[AccidentRecord]): Vector[EnrichedRecord] =
    data
      .filter(r => r.latitude != 0.0 && r.longitude != 0.0)
      .map(enrichRecord)

  def transformPar(data: Vector[AccidentRecord]): Vector[EnrichedRecord] =
    data.par
      .filter(r => r.latitude != 0.0 && r.longitude != 0.0)
      .map(enrichRecord)
      .toVector

  def countRemovedZeroCoordinates(data: Vector[AccidentRecord]): Int =
    data.count(r => r.latitude == 0.0 || r.longitude == 0.0)

  // ---------------------------
  // Summary (Pure)
  // ---------------------------
  def summarizeSeq(data: Vector[EnrichedRecord]): SummaryResult =
    val totalAccidents = data.size
    val totalDeaths = data.map(_.deaths).sum
    val totalSerious = data.map(_.seriousInjuries).sum
    val totalMinor = data.map(_.minorInjuries).sum
    val totalAll = data.map(_.totalInjuries).sum
    val highRiskCases = data.count(_.riskLevel == "High")

    val top5Provinces =
      data.groupBy(_.province)
        .view
        .mapValues(_.size)
        .toList
        .sortBy { case (_, count) => -count }
        .take(5)

    SummaryResult(
      totalAccidents = totalAccidents,
      totalDeaths = totalDeaths,
      totalSeriousInjuries = totalSerious,
      totalMinorInjuries = totalMinor,
      totalAllInjuries = totalAll,
      highRiskCases = highRiskCases,
      top5Provinces = top5Provinces
    )

  def summarizePar(data: Vector[EnrichedRecord]): SummaryResult =
    val totalAccidents = data.par.size
    val totalDeaths = data.par.map(_.deaths).sum
    val totalSerious = data.par.map(_.seriousInjuries).sum
    val totalMinor = data.par.map(_.minorInjuries).sum
    val totalAll = data.par.map(_.totalInjuries).sum
    val highRiskCases = data.par.count(_.riskLevel == "High")

    val provinceCountMap =
      data.par
        .groupBy(_.province)
        .map { case (province, records) => province -> records.size }
        .seq
        .toMap

    val top5Provinces =
      provinceCountMap.toList
        .sortBy { case (_, count) => -count }
        .take(5)

    SummaryResult(
      totalAccidents = totalAccidents,
      totalDeaths = totalDeaths,
      totalSeriousInjuries = totalSerious,
      totalMinorInjuries = totalMinor,
      totalAllInjuries = totalAll,
      highRiskCases = highRiskCases,
      top5Provinces = top5Provinces
    )

  // ---------------------------
  // CSV Output Builders (Pure)
  // ---------------------------
  def csvEscape(s: String): String =
    "\"" + s.replace("\"", "\"\"") + "\""

  def cleanCsvContent(data: Vector[AccidentRecord]): String =
    val header =
      "year,province,cause,weather,deaths,seriousInjuries,minorInjuries,totalInjuries,latitude,longitude"

    val rows =
      data.map { r =>
        s"${r.year},${csvEscape(r.province)},${csvEscape(r.cause)},${csvEscape(r.weather)},${r.deaths},${r.seriousInjuries},${r.minorInjuries},${r.totalInjuries},${r.latitude},${r.longitude}"
      }

    (header +: rows).mkString("\n")

  def transformCsvContent(data: Vector[EnrichedRecord]): String =
    val header =
      "year,province,cause,weather,deaths,seriousInjuries,minorInjuries,totalInjuries,severityScore,riskLevel"

    val rows =
      data.map { r =>
        s"${r.year},${csvEscape(r.province)},${csvEscape(r.cause)},${csvEscape(r.weather)},${r.deaths},${r.seriousInjuries},${r.minorInjuries},${r.totalInjuries},${r.severityScore},${r.riskLevel}"
      }

    (header +: rows).mkString("\n")

  def summaryCsvContent(result: SummaryResult): String =
    val baseRows = Vector(
      "metric,value",
      s"totalAccidents,${result.totalAccidents}",
      s"totalDeaths,${result.totalDeaths}",
      s"totalSeriousInjuries,${result.totalSeriousInjuries}",
      s"totalMinorInjuries,${result.totalMinorInjuries}",
      s"totalAllInjuries,${result.totalAllInjuries}",
      s"highRiskCases,${result.highRiskCases}"
    )

    val provinceRows =
      result.top5Provinces.zipWithIndex.map { case ((province, count), idx) =>
        s"topProvince${idx + 1},${csvEscape(s"$province: $count")}"
      }

    (baseRows ++ provinceRows).mkString("\n")

  // ---------------------------
  // Reporting String Builders (Pure)
  // ---------------------------
  def lineSeparator: String =
    "------------------------------------------------------------"

  def renderCleanReport(report: CleanReport): String =
    s"""FUNCTION: CLEAN DATA

ก่อน Clean : ${report.inputRows} แถว
หลัง Clean : ${report.outputRows} แถว
ข้อมูลที่ถูกตัดออก : ${report.invalidRows} แถว
ตัวอย่างข้อมูลหลัง Clean:
${report.sampleOutput.getOrElse("No cleaned data")}
ความเร็ว -> Seq: ${"%.3f".format(report.seqTimeMs)} ms | Parallel: ${"%.3f".format(report.parTimeMs)} ms
$lineSeparator"""

  def renderTransformReport(report: TransformReport): String =
    s"""FUNCTION: TRANSFORM DATA

ก่อน Transform : ${report.inputRows} แถว
หลัง Transform : ${report.outputRows} แถว
ข้อมูลที่ถูกตัดออก : ${report.removedZeroCoordinates} แถว
ตัวอย่างข้อมูลก่อน Transform:
${report.sampleInput.getOrElse("No clean data")}

ตัวอย่างข้อมูลหลัง Transform:
${report.sampleOutput.getOrElse("No transformed data")}
ความเร็ว -> Seq: ${"%.3f".format(report.seqTimeMs)} ms | Parallel: ${"%.3f".format(report.parTimeMs)} ms
$lineSeparator"""

  def renderSummaryReport(report: SummaryReport): String =
    val provinceText =
      if report.summary.top5Provinces.isEmpty then "  * No province data"
      else
        report.summary.top5Provinces
          .map { case (province, count) => s"  * $province : $count" }
          .mkString("\n")

    s"""FUNCTION: SUMMARY

จำนวนข้อมูลที่นำมาสรุป: ${report.inputRows} แถว

ผลสรุป:
- Total Accidents        : ${report.summary.totalAccidents}
- Total Deaths           : ${report.summary.totalDeaths}
- Total Serious Injuries : ${report.summary.totalSeriousInjuries}
- Total Minor Injuries   : ${report.summary.totalMinorInjuries}
- Total All Injuries     : ${report.summary.totalAllInjuries}
- High Risk Cases        : ${report.summary.highRiskCases}
- Top 5 Provinces:
$provinceText
ความเร็ว -> Seq: ${"%.3f".format(report.seqTimeMs)} ms | Parallel: ${"%.3f".format(report.parTimeMs)} ms
$lineSeparator"""

  def renderFinalOverview(filePath: String, mode: String, rawRows: Int): String =
    s"""=========== ROAD ACCIDENT ETL PIPELINE ===========
ไฟล์ที่ใช้: $filePath
โหมดการทำงาน: $mode
จำนวนข้อมูลดิบก่อนประมวลผล: $rawRows แถว"""

  def renderAllDoneSummary: String =
    s"""สรุปการทำงานทั้งหมด
- Clean: คัดข้อมูลเสียออก และสร้าง cleaned_data.csv
- Transform: เพิ่ม severityScore และ riskLevel และสร้าง transformed_data.csv
- Summary: สรุปสถิติรวม และสร้าง summary_data.csv
- ไฟล์ CSV ต้นฉบับไม่ถูกแก้ไข โปรแกรมอ่านอย่างเดียว
$lineSeparator"""

  // ---------------------------
  // Benchmark Wrapper (Impure by nature of timing,
  // but isolated outside core transformation logic)
  // ---------------------------
  def benchmark[A](block: => A): (A, Double) =
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    val elapsedMs = (end - start) / 1e6
    (result, elapsedMs)

  // ============================================================
  // PIPELINE ORCHESTRATION
  // ============================================================

  def runClean(
      dataLines: Vector[String],
      headerMap: Map[String, Int]
  ): (Vector[AccidentRecord], CleanReport) =
    val invalidRows = countInvalidRows(dataLines, headerMap)

    val (seqResult, seqTime) =
      benchmark(cleanDataSeq(dataLines, headerMap))

    val (parResult, parTime) =
      benchmark(cleanDataPar(dataLines, headerMap))

    val report = CleanReport(
      inputRows = dataLines.size,
      outputRows = seqResult.size,
      invalidRows = invalidRows,
      sampleOutput = seqResult.headOption,
      seqTimeMs = seqTime,
      parTimeMs = parTime,
      sameResult = seqResult == parResult
    )

    (seqResult, report)

  def runTransform(
      cleaned: Vector[AccidentRecord]
  ): (Vector[EnrichedRecord], TransformReport) =
    val removedZeroCoordinates = countRemovedZeroCoordinates(cleaned)

    val (seqResult, seqTime) =
      benchmark(transformSeq(cleaned))

    val (parResult, parTime) =
      benchmark(transformPar(cleaned))

    val report = TransformReport(
      inputRows = cleaned.size,
      outputRows = seqResult.size,
      removedZeroCoordinates = removedZeroCoordinates,
      sampleInput = cleaned.headOption,
      sampleOutput = seqResult.headOption,
      seqTimeMs = seqTime,
      parTimeMs = parTime,
      sameResult = seqResult == parResult
    )

    (seqResult, report)

  def runSummary(
      transformed: Vector[EnrichedRecord]
  ): (SummaryResult, SummaryReport) =
    val (seqResult, seqTime) =
      benchmark(summarizeSeq(transformed))

    val (parResult, parTime) =
      benchmark(summarizePar(transformed))

    val report = SummaryReport(
      inputRows = transformed.size,
      summary = seqResult,
      seqTimeMs = seqTime,
      parTimeMs = parTime,
      sameResult = seqResult == parResult
    )

    (seqResult, report)

  def runPipeline(
      dataLines: Vector[String],
      headerMap: Map[String, Int]
  ): PipelineOutput =
    val (cleaned, cleanReport) = runClean(dataLines, headerMap)
    val (transformed, transformReport) = runTransform(cleaned)
    val (summary, summaryReport) = runSummary(transformed)

    PipelineOutput(
      cleanData = cleaned,
      transformedData = transformed,
      summary = summary,
      cleanReport = cleanReport,
      transformReport = transformReport,
      summaryReport = summaryReport
    )

  // ============================================================
  // I/O SECTION
  // ============================================================

  def readUtf8Lines(filePath: String): Either[String, Vector[String]] =
    Using(Source.fromFile(filePath, "UTF-8")) { source =>
      source.getLines().toVector
    }.toEither.left.map(_.getMessage)

  def writeTextFile(filePath: String, content: String): Either[String, Unit] =
    Using(new PrintWriter(filePath, "UTF-8")) { writer =>
      writer.write(content)
    }.toEither.left.map(_.getMessage)

  def exportOutputs(
      cleaned: Vector[AccidentRecord],
      transformed: Vector[EnrichedRecord],
      summary: SummaryResult
  ): Either[String, Unit] =
    for
      _ <- writeTextFile("cleaned_data.csv", cleanCsvContent(cleaned))
      _ <- writeTextFile("transformed_data.csv", transformCsvContent(transformed))
      _ <- writeTextFile("summary_data.csv", summaryCsvContent(summary))
    yield ()

  // ============================================================
  // MAIN
  // ============================================================
  def main(args: Array[String]): Unit =
    val mode =
      if args.nonEmpty then args(0).toLowerCase
      else "all"

    val filePath = "accidentdataset.csv"

    readUtf8Lines(filePath) match
      case Left(error) =>
        println(s"Error reading file: $error")

      case Right(lines) =>
        if lines.isEmpty then
          println("CSV file is empty.")
        else
          val header = parseCsvLine(lines.head)
          val headerMap = header.zipWithIndex.toMap
          val dataLines = lines.tail

          println(renderFinalOverview(filePath, mode, dataLines.size))
          println()
          println("ตัวอย่างข้อมูลดิบ 1 แถว:")
          println(dataLines.headOption.getOrElse("No data"))
          println(lineSeparator)

          mode match

            case "clean" =>
              val (cleaned, cleanReport) = runClean(dataLines, headerMap)
              println(renderCleanReport(cleanReport))
              writeTextFile("cleaned_data.csv", cleanCsvContent(cleaned))


            case "transform" =>
              val (cleaned, cleanReport) = runClean(dataLines, headerMap)
              val (transformed, transformReport) = runTransform(cleaned)

              println(renderCleanReport(cleanReport))
              println(renderTransformReport(transformReport))

              writeTextFile("cleaned_data.csv", cleanCsvContent(cleaned))
              writeTextFile("transformed_data.csv", transformCsvContent(transformed))


            case "summary" =>
              val (cleaned, cleanReport) = runClean(dataLines, headerMap)
              val (transformed, transformReport) = runTransform(cleaned)
              val (summary, summaryReport) = runSummary(transformed)

              println(renderCleanReport(cleanReport))
              println(renderTransformReport(transformReport))
              println(renderSummaryReport(summaryReport))

              exportOutputs(cleaned, transformed, summary)


            case _ =>
              val (cleaned, cleanReport) = runClean(dataLines, headerMap)
              val (transformed, transformReport) = runTransform(cleaned)
              val (summary, summaryReport) = runSummary(transformed)

              println(renderCleanReport(cleanReport))
              println(renderTransformReport(transformReport))
              println(renderSummaryReport(summaryReport))

              exportOutputs(cleaned, transformed, summary)

              println(renderAllDoneSummary)