# Traffic Accident Data Pipeline — Scala 3

A data processing project demonstrating **functional programming** and **sequential vs. parallel processing** on traffic accident CSV data.

## What it does
- Parse and validate traffic accident records from a CSV dataset
- Clean invalid input and transform records into structured Scala case classes
- Enrich each record with a severity score and risk level
- Summarize accident totals, injuries, fatalities and top provinces
- Compare sequential and parallel execution times and validate that the results agree

## Technology
**Scala 3 · sbt · Scala Parallel Collections · CSV**

## Run
Install a compatible JDK and `sbt`, then:

```bash
sbt compile
sbt run
sbt test
```

The dataset `accidentdataset.csv` is included in the repository.

## Repository layout
- `src/main/scala/Main.scala` — parsing, cleaning, transformations, analysis and benchmarking
- `src/test/scala/` — tests
- `accidentdataset.csv` — sample data for the pipeline
- `build.sbt` — Scala build configuration

## Skills demonstrated
Functional data transformations, immutable case classes, safe parsing with `Option`/`Either`, error handling, parallel collections, and exploratory data aggregation.

> **Note:** This is an educational data processing project; timing results depend on the machine and dataset.
