# Spark-File-Mover for Apache Spark

This plugin provides a custom `OutputCommitter` for Apache Spark that allows for flexible file renaming and moving after job completion.

## Features

- Automatically rename and move output files based on a configurable pattern
- Support for partitioned data
- Preserves Spark's default behavior when conditions for moving files are not met

## Usage

### Configuration

1. When using `spark-submit`:

```bash
spark-submit --conf spark.sql.sources.outputCommitterClass=dev.mauch.spark.MoveFilesOutputCommitter
```

2. When creating a SparkSession in code:

```scala
val spark = SparkSession
  .builder()
  .master("local[*]")
  .config("spark.sql.sources.outputCommitterClass", "dev.mauch.spark.MoveFilesOutputCommitter")
  .getOrCreate()
```

### Writing Data

When writing data, you need to specify the `spark.writer.movefiles` option with a pattern for the target file name:

```scala
df.write
  .option("spark.writer.movefiles", "$outputDirectory/custom_name_$category_$id.csv")
  .mode(SaveMode.Overwrite)
  .csv("output/path")
```

## File Naming Pattern

The `spark.writer.movefiles` option accepts a pattern string that can include variables:

- `$outputDirectory`: The base output directory
- `$<partition_column>`: Any partition column name

For example, `"$outputDirectory/cat_$category_id_$id.csv"` would create files named like `cat_data_id_1.csv` for a row with partitions `category = "data"` and `id = 1`.

## Behavior

1. Files are only moved if
    - the `spark.writer.movefiles` setting is configured
   - there's only one file per target path

2. For partitioned data, each partition is processed separately, allowing for partition-specific file naming.

## Example

```scala
import spark.implicits._

val df = Seq(
  ("data", 1, "foo"),
  ("data", 2, "bar"),
  ("other", 3, "baz")
).toDF("category", "id", "value")

df.write
  .option("spark.writer.movefiles", "$outputDirectory/cat_$category_id_$id.csv")
  .partitionBy("category", "id")
  .csv("output/path")
```

This will create files like:
- `output/path/cat_data_id_1.csv`
- `output/path/cat_data_id_2.csv`
- `output/path/cat_other_id_3.csv`

## Notes

- Ensure that the pattern specified in `spark.writer.movefiles` will result in unique file names for your data to avoid conflicts.
- The `_SUCCESS` file is not moved or renamed.
- The directories created by Spark are not removed automatically
- Always test thoroughly in a safe environment before using in production.
