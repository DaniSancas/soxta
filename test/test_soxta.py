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
                         "<c#><winforms><type-conversion><decimal><opacity>"), result)

    def test_list_fields_from_default_schema(self):
        expected_dict = {'Id': ["int"],
                         'TagName': ["string", "null"],
                         'Count': ["int"],
                         'ExcerptPostId': ["int", "null"],
                         'WikiPostId': ["int", "null"]}

        self.xta.list_fields()
        self.assertEqual(self.xta.schema_fields, expected_dict)

    def test_list_fields_from_schema_with_arrays(self):
        self.assert_.__self__.maxDiff = None
        expected_dict = {u'AcceptedAnswerId': ["int", "null"],
                         u'AnswerCount': ["int", "null"],
                         u'Body': ["string"],
                         u'ClosedDate': ["long", "null"],
                         u'CommentCount': ["int"],
                         u'CommunityOwnedDate': ["long", "null"],
                         u'CreationDate': ["long"],
                         u'FavoriteCount': ["int", "null"],
                         u'Id': ["int"],
                         u'LastActivityDate': ["long"],
                         u'LastEditDate': ["long", "null"],
                         u'LastEditorDisplayName': ["string", "null"],
                         u'LastEditorUserId': ["int", "null"],
                         u'OwnerDisplayName': ["string", "null"],
                         u'OwnerUserId': ["int", "null"],
                         u'ParentId': ["int", "null"],
                         u'PostTypeId': ["int"],
                         u'Score': ["int"],
                         u'Tags': ["array", "null"],
                         u'Title': ["string", "null"],
                         u'ViewCount': ["int", "null"]}

        xmlfile = "data/Sample.xml"
        avroschema = "data/SamplePosts.avsc"
        outputavro = "data/SamplePosts.avro"
        complex_xta = XmlToAvro(xmlfile, avroschema, outputavro)
        complex_xta.list_fields()
        self.assertEqual(complex_xta.schema_fields, expected_dict)
