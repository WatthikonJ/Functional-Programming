// ===============================
// Import Library
// ===============================
import scala.io.Source
import scala.util.Try
import scala.collection.parallel.CollectionConverters._

// ===============================
// Main Object
// ===============================
object Main:

  // ===============================
  // Data Model: ข้อมูลดิบที่ parse จาก CSV แล้ว
  // ===============================
  case class AccidentRecord(
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

  // ===============================
  // Data Model: ข้อมูลหลัง transform แล้ว
  // ===============================
  case class EnrichedRecord(
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

  // ===============================
  // Data Model: ผลลัพธ์สรุป
  // ===============================
  case class SummaryResult(
      totalAccidents: Int,
      totalDeaths: Int,
      totalSeriousInjuries: Int,
      totalMinorInjuries: Int,
      totalAllInjuries: Int,
      highRiskCases: Int,
      top5Provinces: List[(String, Int)]
  )

  // ===============================
  // Extract Stage: แยก CSV line เป็น field
  // รองรับกรณีมี comma ในข้อความที่อยู่ใน quote
  // ===============================
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
        case '"' :: tail => loop(tail, current, !inQuotes, acc)
        case ',' :: tail if !inQuotes => loop(tail, "", inQuotes, acc :+ current)
        case ch :: tail => loop(tail, current + ch, inQuotes, acc)

    loop(line.toList, "", false, Nil)

  // ===============================
  // Safe Parsing: แปลง String -> Int แบบปลอดภัย
  // ถ้าแปลงไม่ได้จะได้ None
  // ===============================
  def safeInt(s: String): Option[Int] =
    Try(s.trim.replace(",", "").toInt).toOption

  // ===============================
  // Safe Parsing: แปลง String -> Double แบบปลอดภัย
  // ===============================
  def safeDouble(s: String): Option[Double] =
    Try(s.trim.replace(",", "").toDouble).toOption

  // ===============================
  // ดึง field จากชื่อ column
  // ===============================
  def getField(
      fields: List[String],
      headerMap: Map[String, Int],
      name: String
  ): Option[String] =
    headerMap.get(name).flatMap(idx => fields.lift(idx))

  // ===============================
  // Clean Stage: parse row -> AccidentRecord
  // ใช้ Either เพื่อแยกแถวดี / แถวเสีย
  // ===============================
  def parseRecord(line: String, headerMap: Map[String, Int]): Either[String, AccidentRecord] =
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
      year,
      province,
      cause,
      weather,
      deaths,
      serious,
      minor,
      total,
      lat,
      lon
    )

    result.toRight(s"Bad row: $line")

  // ===============================
  // Clean Data แบบ Sequential
  // ===============================
  def cleanDataSeq(lines: Vector[String], headerMap: Map[String, Int]): Vector[AccidentRecord] =
    lines.flatMap(line => parseRecord(line, headerMap).toOption)

  // ===============================
  // Clean Data แบบ Parallel
  // ===============================
  def cleanDataPar(lines: Vector[String], headerMap: Map[String, Int]): Vector[AccidentRecord] =
    lines.par.flatMap(line => parseRecord(line, headerMap).toOption).toVector

  // ===============================
  // Transform Function:
  // คำนวณ severityScore และ riskLevel
  // ===============================
  def enrichRecord(r: AccidentRecord): EnrichedRecord =
    val severityScore = (r.deaths * 5) + (r.seriousInjuries * 3) + r.minorInjuries

    val riskLevel =
      if severityScore >= 10 then "High"
      else if severityScore >= 4 then "Medium"
      else "Low"

    EnrichedRecord(
      r.year,
      r.province,
      r.cause,
      r.weather,
      r.deaths,
      r.seriousInjuries,
      r.minorInjuries,
      r.totalInjuries,
      severityScore,
      riskLevel
    )

  // ===============================
  // Transform แบบ Sequential
  // กรอง record ที่พิกัดเป็น 0 ออก
  // ===============================
  def transformSeq(data: Vector[AccidentRecord]): Vector[EnrichedRecord] =
    data
      .filter(r => r.latitude != 0.0 && r.longitude != 0.0)
      .map(enrichRecord)

  // ===============================
  // Transform แบบ Parallel
  // ===============================
  def transformPar(data: Vector[AccidentRecord]): Vector[EnrichedRecord] =
    data.par
      .filter(r => r.latitude != 0.0 && r.longitude != 0.0)
      .map(enrichRecord)
      .toVector

  // ===============================
  // Summary Stage:
  // สรุปจำนวนอุบัติเหตุ ผู้เสียชีวิต ผู้บาดเจ็บ และ top province
  // ===============================
  def summarize(data: Iterable[EnrichedRecord]): SummaryResult =
    val totalAccidents = data.size
    val totalDeaths = data.map(_.deaths).sum
    val totalSerious = data.map(_.seriousInjuries).sum
    val totalMinor = data.map(_.minorInjuries).sum
    val totalAll = data.map(_.totalInjuries).sum
    val highRiskCases = data.count(_.riskLevel == "High")

    val top5Provinces =
      data.groupBy(_.province)
        .map { case (province, records) => (province, records.size) }
        .toList
        .sortBy { case (_, count) => -count }
        .take(5)

    SummaryResult(
      totalAccidents,
      totalDeaths,
      totalSerious,
      totalMinor,
      totalAll,
      highRiskCases,
      top5Provinces
    )

  // ===============================
  // Summary แบบ Sequential
  // ===============================
  def summarySeq(data: Vector[EnrichedRecord]): SummaryResult =
    summarize(data)

  // ===============================
  // Summary แบบ Parallel
  // ใช้ .par.seq เพื่อให้ส่งเข้า summarize ได้
  // ===============================
  def summaryPar(data: Vector[EnrichedRecord]): SummaryResult =
    summarize(data.par.seq)

  // ===============================
  // ฟังก์ชันจับเวลา benchmark
  // ===============================
  def benchmark[A](label: String)(block: => A): (A, Double) =
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    val elapsedMs = (end - start) / 1e6
    println(f"$label%-30s : $elapsedMs%.3f ms")
    (result, elapsedMs)

  // ===============================
  // Main Program
  // ===============================
  def main(args: Array[String]): Unit =
    val filePath =
      if args.nonEmpty then args(0)
      else "accidentdataset.csv"

    val lines = Source.fromFile(filePath, "UTF-8").getLines().toVector

    if lines.isEmpty then
      println("CSV file is empty.")
      sys.exit(1)

    val header = parseCsvLine(lines.head)
    val headerMap = header.zipWithIndex.toMap
    val dataLines = lines.tail

    // ===============================
    // แสดงข้อมูลเริ่มต้น
    // ===============================
    println("===== ROAD ACCIDENT ETL PIPELINE =====")
    println(s"CSV File: $filePath")
    println(s"Total Raw Rows: ${dataLines.size}")
    println(s"Columns Found: ${header.mkString(" | ")}")
    println()

    println("===== SAMPLE RAW DATA =====")
    println(dataLines.headOption.getOrElse("No raw data found"))
    println()

    // ===============================
    // STEP 1: CLEAN
    // ===============================
    println("===== STEP 1: CLEAN DATA =====")

    val (cleanSeqResult, cleanSeqTime) = benchmark("Clean Data Sequential") {
      cleanDataSeq(dataLines, headerMap)
    }

    val (cleanParResult, cleanParTime) = benchmark("Clean Data Parallel") {
      cleanDataPar(dataLines, headerMap)
    }

    println(s"Valid Rows After Clean (Seq): ${cleanSeqResult.size}")
    println(s"Valid Rows After Clean (Par): ${cleanParResult.size}")
    println("Sample Cleaned Record:")
    println(cleanSeqResult.headOption.getOrElse("No cleaned record"))
    println()

    // ===============================
    // STEP 2: TRANSFORM
    // ===============================
    println("===== STEP 2: TRANSFORM DATA =====")

    val (transformSeqResult, transformSeqTime) = benchmark("Transform Sequential") {
      transformSeq(cleanSeqResult)
    }

    val (transformParResult, transformParTime) = benchmark("Transform Parallel") {
      transformPar(cleanParResult)
    }

    println(s"Rows After Transform (Seq): ${transformSeqResult.size}")
    println(s"Rows After Transform (Par): ${transformParResult.size}")
    println("Sample Transformed Record:")
    println(transformSeqResult.headOption.getOrElse("No transformed record"))
    println()

    // ===============================
    // STEP 3: SUMMARY
    // ===============================
    println("===== STEP 3: SUMMARY =====")

    val (summarySeqResult, summarySeqTime) = benchmark("Summary Sequential") {
      summarySeq(transformSeqResult)
    }

    val (summaryParResult, summaryParTime) = benchmark("Summary Parallel") {
      summaryPar(transformParResult)
    }

    // ===============================
    // แสดงผลสรุป
    // ===============================
    println()
    println("===== SUMMARY RESULT =====")
    println(s"Total Accidents        : ${summarySeqResult.totalAccidents}")
    println(s"Total Deaths           : ${summarySeqResult.totalDeaths}")
    println(s"Total Serious Injuries : ${summarySeqResult.totalSeriousInjuries}")
    println(s"Total Minor Injuries   : ${summarySeqResult.totalMinorInjuries}")
    println(s"Total All Injuries     : ${summarySeqResult.totalAllInjuries}")
    println(s"High Risk Cases        : ${summarySeqResult.highRiskCases}")

    println()
    println("Top 5 Provinces:")
    summarySeqResult.top5Provinces.foreach { case (province, count) =>
      println(s"- $province : $count")
    }

    // ===============================
    // แสดงเวลาเปรียบเทียบ
    // ===============================
    println()
    println("===== TIME COMPARISON =====")
    println(f"Clean Data   -> Seq: $cleanSeqTime%.3f ms | Par: $cleanParTime%.3f ms")
    println(f"Transform    -> Seq: $transformSeqTime%.3f ms | Par: $transformParTime%.3f ms")
    println(f"Summary      -> Seq: $summarySeqTime%.3f ms | Par: $summaryParTime%.3f ms")

    // ===============================
    // เช็กว่าผลลัพธ์ Seq กับ Par เท่ากันไหม
    // ===============================
    println()
    println("===== RESULT CHECK =====")
    println(s"Clean Result Equal     : ${cleanSeqResult == cleanParResult}")
    println(s"Transform Result Equal : ${transformSeqResult == transformParResult}")
    println(s"Summary Result Equal   : ${summarySeqResult == summaryParResult}")

    // ===============================
    // อธิบายผลเบื้องต้น
    // ===============================
    println()
    println("===== INTERPRETATION =====")
    println("- Sequential คือประมวลผลทีละลำดับ")
    println("- Parallel คือแบ่งงานไปหลายส่วนเพื่อทำพร้อมกัน")
    println("- ถ้า Parallel เร็วกว่า แปลว่างานนั้นเหมาะกับการกระจายงาน")
    println("- ถ้า Parallel ช้ากว่า อาจเกิด overhead จากการแบ่งงานและรวมผล")