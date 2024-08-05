```bash
spark-submit --conf spark.sql.sources.outputCommitterClass=dev.mauch.spark.MoveFilesOutputCommitter
```


```scala
val spark = SparkSession
  .builder()
  .master("local[*]")
  .config("spark.sql.sources.outputCommitterClass", "dev.mauch.spark.MoveFilesOutputCommitter")
  .getOrCreate()
```