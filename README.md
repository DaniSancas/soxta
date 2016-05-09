# soxta
SOXTA - StackOverflow's XML To AVRO

## How to run
Firstly, we need uncompressed data from the **Archive.org** public dataset: https://archive.org/details/stackexchange. Given the next folder structure, the data should be inside `xml_data` folder. The `avro_schemas` folder should contain the schema files in order to convert from XML to AVRO. The `avro_data` folder should be empty:

```bash
.
├── avro_data       # Empty, to store Avro converted files
├── avro_schemas    # Avro schema files
└── xml_data        # Uncompressed data from Archive.org
```

### Convert from XML to AVRO:

Then we need to run `soxta.py` script, specifying the XML file to convert, the AVRO schema file, and the path for the result AVRO file::

```bash
$ soxta.py xml_data/Posts.xml avro_schemas/Posts.avsc avro_data/Posts.avro

```
