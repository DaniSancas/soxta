#!/usr/bin/python

import avro.schema

from sys import argv
from os.path import isfile
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


class XmlToAvro:

    def __init__(self, xmlfile, avroschema, outputavro):
        """Setup, check if files exists, create schema and writer handler"""
        if not isfile(xmlfile):
            raise IOError
        else:
            self.xmlfile = xmlfile
        self.schema = avro.schema.parse(open(avroschema).read())
        self.writer = DataFileWriter(open(outputavro, "w"), DatumWriter(), self.schema, codec="snappy")


if __name__ == "__main__":
    xta = XmlToAvro(argv[1], argv[2], argv[3])
