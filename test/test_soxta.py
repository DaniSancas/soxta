#!/usr/bin/python
# coding=utf-8

import unittest
from soxta.soxta import XmlToAvro


class TestXmlToAvro(unittest.TestCase):

    def setUp(self):
        """Setup XmlToAvro sample object"""
        xmlfile = "data/Sample.xml"
        avroschema = "data/Sample.avsc"
        outputavro = "data/Sample.avro"
        self.xta = XmlToAvro(xmlfile, avroschema, outputavro)  # It should give no error

    def test_XmlToAvro_class_creation_no_params(self):
        """Not enough parameters provided"""
        self.assertRaises(TypeError, XmlToAvro)

    def test_XmlToAvro_class_creation_one_param(self):
        """Not enough parameters provided"""
        self.assertRaises(TypeError, XmlToAvro, "")

    def test_XmlToAvro_class_creation_two_params(self):
        """Not enough parameters provided"""
        self.assertRaises(TypeError, XmlToAvro, "", "")

    def test_XmlToAvro_class_creation_pass(self):
        """Set path to files"""
        xmlfile = "data/Sample.xml"
        avroschema = "data/Sample.avsc"
        outputavro = "data/Sample.avro"
        XmlToAvro(xmlfile, avroschema, outputavro)  # It should give no error

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

    def test_field_to_datum_string_from_string(self):
        """Evaluates if given string attribute value matches string"""
        types_list = ["string"]
        self.assertIsInstance(self.xta.field_to_datum(types_list, "something"), str)

    def test_field_to_datum_string_from_int(self):
        """Evaluates if given integer attribute value matches string"""
        types_list = ["string"]
        self.assertIsInstance(self.xta.field_to_datum(types_list, 15), str)

    def test_field_to_datum_string_from_positive_int(self):
        """Evaluates if given integer attribute value matches string"""
        types_list = ["string"]
        self.assertEqual(self.xta.field_to_datum(types_list, 15), "15")

    def test_field_to_datum_string_from_negative_int(self):
        """Evaluates if given negative integer attribute value matches string"""
        types_list = ["string"]
        self.assertEqual(self.xta.field_to_datum(types_list, -15), "-15")

    def test_field_to_datum_string_from_boolean_true(self):
        """Evaluates if given boolean attribute value matches string"""
        types_list = ["string"]
        self.assertIsInstance(self.xta.field_to_datum(types_list, True), str)

    def test_field_to_datum_string_from_boolean_true_string(self):
        """Evaluates if given boolean attribute value matches boolean string"""
        types_list = ["string"]
        self.assertEqual(self.xta.field_to_datum(types_list, True), "True")

    def test_field_to_datum_string_from_boolean_false(self):
        """Evaluates if given boolean attribute value matches string"""
        types_list = ["string"]
        self.assertIsInstance(self.xta.field_to_datum(types_list, False), str)

    def test_field_to_datum_string_from_boolean_false_string(self):
        """Evaluates if given boolean attribute value matches booleanstring"""
        types_list = ["string"]
        self.assertEqual(self.xta.field_to_datum(types_list, False), "False")

    def test_field_to_datum_positive_int_from_positive_int(self):
        """Evaluates if given integer attribute value matches integer"""
        types_list = ["int"]
        self.assertIsInstance(self.xta.field_to_datum(types_list, 15), int)

    def test_field_to_datum_positive_int_value_from_positive_int(self):
        """Evaluates if given integer attribute value matches integer"""
        types_list = ["int"]
        self.assertEqual(self.xta.field_to_datum(types_list, 15), 15)

    def test_field_to_datum_negative_int_from_negative_int(self):
        """Evaluates if given negative integer attribute value matches integer"""
        types_list = ["int"]
        self.assertIsInstance(self.xta.field_to_datum(types_list, -15), int)

    def test_field_to_datum_negative_int_value_from_negative_int(self):
        """Evaluates if given negative integer attribute value matches integer"""
        types_list = ["int"]
        self.assertEqual(self.xta.field_to_datum(types_list, -15), -15)

    def test_field_to_datum_positive_int_from_number_string(self):
        """Evaluates if given attribute value matches integer"""
        types_list = ["int"]
        self.assertIsInstance(self.xta.field_to_datum(types_list, "15"), int)

    def test_field_to_datum_negative_int_from_number_string(self):
        """Evaluates if given attribute value matches integer"""
        types_list = ["int"]
        self.assertIsInstance(self.xta.field_to_datum(types_list, "-15"), int)

    def test_field_to_datum_int_from_non_number_string(self):
        """Evaluates if given attribute value matches integer"""
        types_list = ["int"]
        self.assertRaises(ValueError, self.xta.field_to_datum, types_list, "something")

    def test_field_to_datum_boolean_from_true_capitalized_string(self):
        """Evaluates if given true capitalized attribute value matches boolean true"""
        types_list = ["boolean"]
        self.assertTrue(self.xta.field_to_datum(types_list, "True"))

    def test_field_to_datum_boolean_from_false_capitalized_string(self):
        """Evaluates if given false capitalized attribute value matches boolean false"""
        types_list = ["boolean"]
        self.assertFalse(self.xta.field_to_datum(types_list, "False"))

    def test_field_to_datum_boolean_from_true_lower_string(self):
        """Evaluates if given true lower attribute value matches boolean"""
        types_list = ["boolean"]
        self.assertTrue(self.xta.field_to_datum(types_list,  "true"))

    def test_field_to_datum_boolean_from_false_lower_string(self):
        """Evaluates if given false lower attribute value matches boolean"""
        types_list = ["boolean"]
        self.assertFalse(self.xta.field_to_datum(types_list, "false"))

    def test_field_to_datum_boolean_from_true_upper_string(self):
        """Evaluates if given true upper attribute value matches boolean"""
        types_list = ["boolean"]
        self.assertTrue(self.xta.field_to_datum(types_list, "TRUE"))

    def test_field_to_datum_boolean_from_false_upper_string(self):
        """Evaluates if given false upper attribute value matches boolean"""
        types_list = ["boolean"]
        self.assertFalse(self.xta.field_to_datum(types_list, "FALSE"))

    def test_field_to_datum_boolean_from_non_boolean_string(self):
        """Evaluates if given string attribute value matches boolean"""
        types_list = ["boolean"]
        self.assertRaises(ValueError, self.xta.field_to_datum, types_list, "something")

    def test_field_to_datum_boolean_from_non_boolean_int(self):
        """Evaluates if given integer attribute value matches boolean"""
        types_list = ["boolean"]
        self.assertRaises(AttributeError, self.xta.field_to_datum, types_list, 15)  # In 1º place, can't be capitalized

    def test_field_to_datum_long_from_datetime_string(self):
        """Evaluates if given datetime string matches long"""
        types_list = ["long"]
        self.assertEqual(self.xta.field_to_datum(types_list, "2008-08-01T05:12:44.193"), 1217567564)

    def test_field_to_datum_long_from_long_string(self):
        """Evaluates if given long string matches long"""
        types_list = ["long"]
        self.assertEqual(self.xta.field_to_datum(types_list, "1217567564"), 1217567564)

    def test_field_to_datum_long_from_string(self):
        """Evaluates if given long string matches long"""
        types_list = ["long"]
        self.assertRaises(ValueError, self.xta.field_to_datum, types_list, "something")

    def test_field_to_datum_array_from_string(self):
        """Evaluates if given long string matches long"""
        types_list = ["array"]
        result = ['c#', 'winforms', 'type-conversion', 'decimal', 'opacity']
        self.assertEqual(self.xta.field_to_datum(types_list,
                         "&lt;c#&gt;&lt;winforms&gt;&lt;type-conversion&gt;&lt;decimal&gt;&lt;opacity&gt;"), result)

    def test_list_fields_from_default_schema(self):
        expected_dict = {'Id': ["int"],
                         'TagName': ["string", "null"],
                         'Count': ["int"],
                         'ExcerptPostId': ["int", "null"],
                         'WikiPostId': ["int", "null"]}

        self.xta.list_fields()
        self.assertEqual(self.xta.schema_fields, expected_dict)

    def test_list_fields_from_schema_with_arrays(self):
        expected_dict = {'AcceptedAnswerId': ["int", "null"],
                         'AnswerCount': ["int", "null"],
                         'Body': ["string"],
                         'ClosedDate': ["long", "null"],
                         'CommentCount': ["int"],
                         'CommunityOwnedDate': ["long", "null"],
                         'CreationDate': ["long"],
                         'FavoriteCount': ["int", "null"],
                         'Id': ["int"],
                         'LastActivityDate': ["long"],
                         'LastEditDate': ["long", "null"],
                         'LastEditorDisplayName': ["string", "null"],
                         'LastEditorUserId': ["int", "null"],
                         'OwnerDisplayName': ["string", "null"],
                         'OwnerUserId': ["int", "null"],
                         'ParentId': ["int", "null"],
                         'PostTypeId': ["int"],
                         'Score': ["int"],
                         'Tags': ["null", "array"],
                         'Title': ["string", "null"],
                         'ViewCount': ["int", "null"]}

        xmlfile = "data/Sample.xml"
        avroschema = "data/Sample2.avsc"
        outputavro = "data/Sample.avro"
        complex_xta = XmlToAvro(xmlfile, avroschema, outputavro)
        complex_xta.list_fields()
        self.assertEqual(complex_xta.schema_fields, expected_dict)
