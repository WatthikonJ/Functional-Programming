# Traffic Accident Data Pipeline — Scala 3

A traffic accident data analysis project using functional programming and sequential/parallel processing.

## Features
- Read and validate traffic accident records from CSV
- Clean and transform data into Scala case classes
- Calculate severity scores and risk levels
- Summarize accident totals, injuries, deaths and provinces
- Compare sequential and parallel processing results

## Tech Stack
- Scala 3
- sbt
- Scala Parallel Collections
- CSV

## Project Structure
- `src/main/scala/Main.scala` — data processing and analysis
- `src/test/scala/` — tests
- `accidentdataset.csv` — dataset
- `build.sbt` — build configuration

## Run Locally

```bash
sbt compile
sbt run
sbt test
```
