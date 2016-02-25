#!/usr/bin/python
# coding=utf-8

import ast
import calendar
import iso8601
import avro.schema

from sys import argv
from os.path import isfile
from lxml import etree
from avro.datafile import DataFileWriter
from avro.io import DatumWriter, json


class AttributeNotExpectedException(Exception):
    def __init__(self, schema, attribute, row=None):
        pretty_schema = json.dumps(json.loads(str(schema)), indent=2)
        if row:
            self.message = "Attribute {} from row {} is not expected in schema {}"\
                           .format(attribute, str(row), pretty_schema)
        else:
            self.message = "Attribute {} is not expected in schema {}".format(attribute, pretty_schema)


class FieldTypeNotExpectedException(Exception):
    def __init__(self, schema, field_type, types_list):
        pretty_schema = json.dumps(json.loads(str(schema)), indent=2)
        self.message = "Field type {} from {} list not expected. Schema dump: {}"\
                       .format(field_type, types_list, pretty_schema)


class XmlToAvro:

    def __init__(self, xmlfile, avroschema, outputavro):
        """Setup, check if files exists, create schema and writer handler"""
        if not isfile(xmlfile):
            raise IOError("File {} doesn't exist.".format(xmlfile))
        else:
            self.xmlfile = xmlfile
        self.schema = avro.schema.parse(open(avroschema).read())
        self.writer = DataFileWriter(open(outputavro, "w"), DatumWriter(), self.schema, codec="snappy")
        self.schema_fields = {}  # Init expected fields for the schema
        self.list_fields()

    def list_fields(self):
        for field in self.schema.fields:
            self.schema_fields[field.name] = self.field_types_list(field.type)

    def transform(self):
        """Reads the XML row by row, transforms data to AVRO and deletes last row from memory"""
        context = etree.iterparse(self.xmlfile, tag='row')
        for event, element in context:
            self.writer.append(self.process_xml_row(element))
            element.clear()
            while element.getprevious() is not None:
                del element.getparent()[0]
        del context

    def process_xml_row(self, element):
        """For each row, iterate attributes and add key-value pairs to datum. Then return datum"""
        datum = {}
        for attribute_name, attribute_value in element.attrib.items():
            for field in self.schema.fields:
                if field.name == attribute_name:
                    available_types_list = self.schema_fields[field.name]  # Gets all available types for given field
                    datum[attribute_name] = self.field_to_datum(available_types_list, attribute_value)
                    break
            else:
                raise AttributeNotExpectedException(self.schema, attribute_name, element)
        return datum

    @staticmethod
    def field_types_list(field_type):
        """Simplifies list by always returning a list, whether is a string, or a list"""
        if isinstance(field_type, avro.schema.UnionSchema):  # Is a list
            return [str(schematype.props["type"]).lower() for schematype in field_type.schemas]
        else:  # Is a single field
            return [str(field_type.type).lower()]

    def field_to_datum(self, types_list, attribute_value):
        """Iterates over available types list and returns attribute value in the proper format"""
        for item_type in types_list:
            if item_type == "string":
                return attribute_value
            elif item_type == "int":
                return int(attribute_value)
            elif item_type == "boolean":
                return ast.literal_eval(attribute_value.capitalize())
            elif item_type == "long":
                if "T" in attribute_value:  # It's an iso8601 date
                    return int(calendar.timegm(iso8601.parse_date(attribute_value).timetuple()))
                else:
                    return long(attribute_value)
            elif item_type == "array":
                if "&lt;" in attribute_value or "&gt;" in attribute_value:  # Tag array
                    without_start = attribute_value.replace("&lt;", "")
                    split_string = without_start.split("&gt;")
                    split_string.remove("")  # Possible remaining empty string
                    return split_string
            elif item_type == "null":
                pass  # Do nothing
            else:
                FieldTypeNotExpectedException(self.schema, item_type, types_list)  # Schema contains non expected field


if __name__ == "__main__":
    xta = XmlToAvro(argv[1], argv[2], argv[3])
    xta.transform()
