#!/usr/bin/python

import unittest
from soxta.soxta import XmlToAvro


class TestXmlToAvro(unittest.TestCase):

    def test_XmlToAvro_class_creation_not_enough_params(self):
        """Not enough parameters provided"""
        self.assertRaises(TypeError, XmlToAvro)
        self.assertRaises(TypeError, XmlToAvro, "")
        self.assertRaises(TypeError, XmlToAvro, "", "")

    def test_XmlToAvro_class_creation_non_existing_xml(self):
        """Set wrong path to files"""
        xmlfile = "path/to/non/existing/Sample.xml"
        avroschema = "data/Sample.avsc"
        outputavro = "data/Sample.avro"
        self.assertRaises(IOError, XmlToAvro, xmlfile, avroschema, outputavro)

    def test_XmlToAvro_class_creation_non_existing_avsc(self):
        """Set path to files"""
        xmlfile = "data/Sample.xml"
        avroschema = "path/to/non/existing/Sample.avsc"
        outputavro = "data/Sample.avro"
        self.assertRaises(IOError, XmlToAvro, xmlfile, avroschema, outputavro)

    def test_XmlToAvro_class_creation_pass(self):
        """Set path to files"""
        xmlfile = "data/Sample.xml"
        avroschema = "data/Sample.avsc"
        outputavro = "data/Sample.avro"
        XmlToAvro(xmlfile, avroschema, outputavro)  # It should give no error
