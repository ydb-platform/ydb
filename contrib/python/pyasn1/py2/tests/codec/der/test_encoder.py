#
# This file is part of pyasn1 software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://pyasn1.readthedocs.io/en/latest/license.html
#
import sys
import unittest

from __tests__.base import BaseTestCase

from pyasn1.type import tag
from pyasn1.type import namedtype
from pyasn1.type import opentype
from pyasn1.type import univ
from pyasn1.codec.der import encoder
from pyasn1.compat.octets import ints2octs


class OctetStringEncoderTestCase(BaseTestCase):
    def testDefModeShort(self):
        assert encoder.encode(
            univ.OctetString('Quick brown fox')
        ) == ints2octs((4, 15, 81, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 32, 102, 111, 120))

    def testDefModeLong(self):
        assert encoder.encode(
            univ.OctetString('Q' * 10000)
        ) == ints2octs((4, 130, 39, 16) + (81,) * 10000)


class BitStringEncoderTestCase(BaseTestCase):
    def testDefModeShort(self):
        assert encoder.encode(
            univ.BitString((1,))
        ) == ints2octs((3, 2, 7, 128))

    def testDefModeLong(self):
        assert encoder.encode(
            univ.BitString((1,) * 80000)
        ) == ints2octs((3, 130, 39, 17, 0) + (255,) * 10000)


class SetOfEncoderTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        self.s = univ.SetOf(componentType=univ.OctetString())

    def testDefMode1(self):
        self.s.clear()
        self.s.append('a')
        self.s.append('ab')

        assert encoder.encode(self.s) == ints2octs((49, 7, 4, 1, 97, 4, 2, 97, 98))

    def testDefMode2(self):
        self.s.clear()
        self.s.append('ab')
        self.s.append('a')

        assert encoder.encode(self.s) == ints2octs((49, 7, 4, 1, 97, 4, 2, 97, 98))

    def testDefMode3(self):
        self.s.clear()
        self.s.append('b')
        self.s.append('a')

        assert encoder.encode(self.s) == ints2octs((49, 6, 4, 1, 97, 4, 1, 98))

    def testDefMode4(self):
        self.s.clear()
        self.s.append('a')
        self.s.append('b')

        assert encoder.encode(self.s) == ints2octs((49, 6, 4, 1, 97, 4, 1, 98))


class SetWithAlternatingChoiceEncoderTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        c = univ.Choice(componentType=namedtype.NamedTypes(
            namedtype.NamedType('name', univ.OctetString()),
            namedtype.NamedType('amount', univ.Boolean()))
        )

        self.s = univ.Set(componentType=namedtype.NamedTypes(
            namedtype.NamedType('value', univ.Integer(5)),
            namedtype.NamedType('status', c))
        )

    def testComponentsOrdering1(self):
        self.s.setComponentByName('status')
        self.s.getComponentByName('status').setComponentByPosition(0, 'A')
        assert encoder.encode(self.s) == ints2octs((49, 6, 2, 1, 5, 4, 1, 65))

    def testComponentsOrdering2(self):
        self.s.setComponentByName('status')
        self.s.getComponentByName('status').setComponentByPosition(1, True)
        assert encoder.encode(self.s) == ints2octs((49, 6, 1, 1, 255, 2, 1, 5))


class SetWithTaggedChoiceEncoderTestCase(BaseTestCase):

    def testWithUntaggedChoice(self):

        c = univ.Choice(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('premium', univ.Boolean())
            )
        )

        s = univ.Set(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('name', univ.OctetString()),
                namedtype.NamedType('customer', c)
            )
        )

        s.setComponentByName('name', 'A')
        s.getComponentByName('customer').setComponentByName('premium', True)

        assert encoder.encode(s) == ints2octs((49, 6, 1, 1, 255, 4, 1, 65))

    def testWithTaggedChoice(self):

        c = univ.Choice(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('premium', univ.Boolean())
            )
        ).subtype(implicitTag=tag.Tag(tag.tagClassContext, tag.tagFormatConstructed, 7))

        s = univ.Set(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('name', univ.OctetString()),
                namedtype.NamedType('customer', c)
            )
        )

        s.setComponentByName('name', 'A')
        s.getComponentByName('customer').setComponentByName('premium', True)

        assert encoder.encode(s) == ints2octs((49, 8, 4, 1, 65, 167, 3, 1, 1, 255))


class SequenceEncoderWithUntaggedOpenTypesTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        openType = opentype.OpenType(
            'id',
            {1: univ.Integer(),
             2: univ.OctetString()}
        )
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('id', univ.Integer()),
                namedtype.NamedType('blob', univ.Any(), openType=openType)
            )
        )

    def testEncodeOpenTypeChoiceOne(self):
        self.s.clear()

        self.s[0] = 1
        self.s[1] = univ.Integer(12)

        assert encoder.encode(self.s, asn1Spec=self.s) == ints2octs(
            (48, 5, 2, 1, 1, 49, 50)
        )

    def testEncodeOpenTypeChoiceTwo(self):
        self.s.clear()

        self.s[0] = 2
        self.s[1] = univ.OctetString('quick brown')

        assert encoder.encode(self.s, asn1Spec=self.s) == ints2octs(
            (48, 14, 2, 1, 2, 113, 117, 105, 99, 107, 32,
             98, 114, 111, 119, 110)
        )

    def testEncodeOpenTypeUnknownId(self):
        self.s.clear()

        self.s[0] = 2
        self.s[1] = univ.ObjectIdentifier('1.3.6')

        try:
            encoder.encode(self.s, asn1Spec=self.s)

        except PyAsn1Error:
            assert False, 'incompatible open type tolerated'

    def testEncodeOpenTypeIncompatibleType(self):
        self.s.clear()

        self.s[0] = 2
        self.s[1] = univ.ObjectIdentifier('1.3.6')

        try:
            encoder.encode(self.s, asn1Spec=self.s)

        except PyAsn1Error:
            assert False, 'incompatible open type tolerated'


class SequenceEncoderWithImplicitlyTaggedOpenTypesTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        openType = opentype.OpenType(
            'id',
            {1: univ.Integer(),
             2: univ.OctetString()}
        )
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('id', univ.Integer()),
                namedtype.NamedType('blob', univ.Any().subtype(implicitTag=tag.Tag(tag.tagClassContext, tag.tagFormatSimple, 3)), openType=openType)
            )
        )

    def testEncodeOpenTypeChoiceOne(self):
        self.s.clear()

        self.s[0] = 1
        self.s[1] = univ.Integer(12)

        assert encoder.encode(self.s, asn1Spec=self.s) == ints2octs(
            (48, 9, 2, 1, 1, 131, 4, 131, 2, 49, 50)
        )


class SequenceEncoderWithExplicitlyTaggedOpenTypesTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        openType = opentype.OpenType(
            'id',
            {1: univ.Integer(),
             2: univ.OctetString()}
        )
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('id', univ.Integer()),
                namedtype.NamedType('blob', univ.Any().subtype(explicitTag=tag.Tag(tag.tagClassContext, tag.tagFormatSimple, 3)), openType=openType)
            )
        )

    def testEncodeOpenTypeChoiceOne(self):
        self.s.clear()

        self.s[0] = 1
        self.s[1] = univ.Integer(12)

        assert encoder.encode(self.s, asn1Spec=self.s) == ints2octs(
            (48, 9, 2, 1, 1, 163, 4, 163, 2, 49, 50)
        )


class SequenceEncoderWithUntaggedSetOfOpenTypesTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        openType = opentype.OpenType(
            'id',
            {1: univ.Integer(),
             2: univ.OctetString()}
        )
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('id', univ.Integer()),
                namedtype.NamedType('blob', univ.SetOf(
                    componentType=univ.Any()), openType=openType)
            )
        )

    def testEncodeOpenTypeChoiceOne(self):
        self.s.clear()

        self.s[0] = 1
        self.s[1].append(univ.Integer(12))

        assert encoder.encode(self.s, asn1Spec=self.s) == ints2octs(
            (48, 7, 2, 1, 1, 49, 2, 49, 50)
        )

    def testEncodeOpenTypeChoiceTwo(self):
        self.s.clear()

        self.s[0] = 2
        self.s[1].append(univ.OctetString('quick brown'))

        assert encoder.encode(self.s, asn1Spec=self.s) == ints2octs(
            (48, 16, 2, 1, 2, 49, 11, 113, 117, 105, 99, 107, 32, 98, 114,
             111, 119, 110)
        )

    def testEncodeOpenTypeUnknownId(self):
        self.s.clear()

        self.s[0] = 2
        self.s[1].append(univ.ObjectIdentifier('1.3.6'))

        try:
            encoder.encode(self.s, asn1Spec=self.s)

        except PyAsn1Error:
            assert False, 'incompatible open type tolerated'

    def testEncodeOpenTypeIncompatibleType(self):
        self.s.clear()

        self.s[0] = 2
        self.s[1].append(univ.ObjectIdentifier('1.3.6'))

        try:
            encoder.encode(self.s, asn1Spec=self.s)

        except PyAsn1Error:
            assert False, 'incompatible open type tolerated'


class SequenceEncoderWithImplicitlyTaggedSetOfOpenTypesTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        openType = opentype.OpenType(
            'id',
            {1: univ.Integer(),
             2: univ.OctetString()}
        )
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('id', univ.Integer()),
                namedtype.NamedType('blob', univ.SetOf(
                    componentType=univ.Any().subtype(
                        implicitTag=tag.Tag(
                            tag.tagClassContext, tag.tagFormatSimple, 3))),
                    openType=openType)
            )
        )

    def testEncodeOpenTypeChoiceOne(self):
        self.s.clear()

        self.s[0] = 1
        self.s[1].append(univ.Integer(12))

        assert encoder.encode(self.s, asn1Spec=self.s) == ints2octs(
            (48, 11, 2, 1, 1, 49, 6, 131, 4, 131, 2, 49, 50)
        )


class SequenceEncoderWithExplicitlyTaggedSetOfOpenTypesTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        openType = opentype.OpenType(
            'id',
            {1: univ.Integer(),
             2: univ.OctetString()}
        )
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('id', univ.Integer()),
                namedtype.NamedType('blob', univ.SetOf(
                    componentType=univ.Any().subtype(
                        explicitTag=tag.Tag(tag.tagClassContext, tag.tagFormatSimple, 3))),
                    openType=openType)
            )
        )

    def testEncodeOpenTypeChoiceOne(self):
        self.s.clear()

        self.s[0] = 1
        self.s[1].append(univ.Integer(12))

        assert encoder.encode(self.s, asn1Spec=self.s) == ints2octs(
            (48, 11, 2, 1, 1, 49, 6, 163, 4, 163, 2, 49, 50)
        )


class NestedOptionalSequenceEncoderTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        inner = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('first-name', univ.OctetString()),
                namedtype.DefaultedNamedType('age', univ.Integer(33)),
            )
        )

        outerWithOptional = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('inner', inner),
            )
        )

        outerWithDefault = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.DefaultedNamedType('inner', inner),
            )
        )

        self.s1 = outerWithOptional
        self.s2 = outerWithDefault

    def __initOptionalWithDefaultAndOptional(self):
        self.s1.clear()
        self.s1[0][0] = 'test'
        self.s1[0][1] = 123
        return self.s1

    def __initOptionalWithDefault(self):
        self.s1.clear()
        self.s1[0][1] = 123
        return self.s1

    def __initOptionalWithOptional(self):
        self.s1.clear()
        self.s1[0][0] = 'test'
        return self.s1

    def __initOptional(self):
        self.s1.clear()
        return self.s1

    def __initDefaultWithDefaultAndOptional(self):
        self.s2.clear()
        self.s2[0][0] = 'test'
        self.s2[0][1] = 123
        return self.s2

    def __initDefaultWithDefault(self):
        self.s2.clear()
        self.s2[0][0] = 'test'
        return self.s2

    def __initDefaultWithOptional(self):
        self.s2.clear()
        self.s2[0][1] = 123
        return self.s2

    def testDefModeOptionalWithDefaultAndOptional(self):
        s = self.__initOptionalWithDefaultAndOptional()
        assert encoder.encode(s) == ints2octs((48, 11, 48, 9, 4, 4, 116, 101, 115, 116, 2, 1, 123))

    def testDefModeOptionalWithDefault(self):
        s = self.__initOptionalWithDefault()
        assert encoder.encode(s) == ints2octs((48, 5, 48, 3, 2, 1, 123))

    def testDefModeOptionalWithOptional(self):
        s = self.__initOptionalWithOptional()
        assert encoder.encode(s) == ints2octs((48, 8, 48, 6, 4, 4, 116, 101, 115, 116))

    def testDefModeOptional(self):
        s = self.__initOptional()
        assert encoder.encode(s) == ints2octs((48, 0))

    def testDefModeDefaultWithDefaultAndOptional(self):
        s = self.__initDefaultWithDefaultAndOptional()
        assert encoder.encode(s) == ints2octs((48, 11, 48, 9, 4, 4, 116, 101, 115, 116, 2, 1, 123))

    def testDefModeDefaultWithDefault(self):
        s = self.__initDefaultWithDefault()
        assert encoder.encode(s) == ints2octs((48, 8, 48, 6, 4, 4, 116, 101, 115, 116))

    def testDefModeDefaultWithOptional(self):
        s = self.__initDefaultWithOptional()
        assert encoder.encode(s) == ints2octs((48, 5, 48, 3, 2, 1, 123))


class NestedOptionalChoiceEncoderTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        layer3 = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('first-name', univ.OctetString()),
                namedtype.DefaultedNamedType('age', univ.Integer(33)),
            )
        )

        layer2 = univ.Choice(
            componentType=namedtype.NamedTypes(
                namedtype.NamedType('inner', layer3),
                namedtype.NamedType('first-name', univ.OctetString())
            )
        )

        layer1 = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('inner', layer2),
            )
        )

        self.s = layer1

    def __initOptionalWithDefaultAndOptional(self):
        self.s.clear()
        self.s[0][0][0] = 'test'
        self.s[0][0][1] = 123
        return self.s

    def __initOptionalWithDefault(self):
        self.s.clear()
        self.s[0][0][1] = 123
        return self.s

    def __initOptionalWithOptional(self):
        self.s.clear()
        self.s[0][0][0] = 'test'
        return self.s

    def __initOptional(self):
        self.s.clear()
        return self.s

    def testDefModeOptionalWithDefaultAndOptional(self):
        s = self.__initOptionalWithDefaultAndOptional()
        assert encoder.encode(s) == ints2octs((48, 11, 48, 9, 4, 4, 116, 101, 115, 116, 2, 1, 123))

    def testDefModeOptionalWithDefault(self):
        s = self.__initOptionalWithDefault()
        assert encoder.encode(s) == ints2octs((48, 5, 48, 3, 2, 1, 123))

    def testDefModeOptionalWithOptional(self):
        s = self.__initOptionalWithOptional()
        assert encoder.encode(s) == ints2octs((48, 8, 48, 6, 4, 4, 116, 101, 115, 116))

    def testDefModeOptional(self):
        s = self.__initOptional()
        assert encoder.encode(s) == ints2octs((48, 0))


class NestedOptionalSequenceOfEncoderTestCase(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)

        layer2 = univ.SequenceOf(
            componentType=univ.OctetString()
        )

        layer1 = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('inner', layer2),
            )
        )

        self.s = layer1

    def __initOptionalWithValue(self):
        self.s.clear()
        self.s[0][0] = 'test'
        return self.s

    def __initOptional(self):
        self.s.clear()
        return self.s

    def testDefModeOptionalWithValue(self):
        s = self.__initOptionalWithValue()
        assert encoder.encode(s) == ints2octs((48, 8, 48, 6, 4, 4, 116, 101, 115, 116))

    def testDefModeOptional(self):
        s = self.__initOptional()
        assert encoder.encode(s) == ints2octs((48, 0))


class EmptyInnerFieldOfSequenceEncoderTestCase(BaseTestCase):

    def testInitializedOptionalNullIsEncoded(self):
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('null', univ.Null())
            )
        )

        self.s.clear()
        self.s[0] = ''
        assert encoder.encode(self.s) == ints2octs((48, 2, 5, 0))

    def testUninitializedOptionalNullIsNotEncoded(self):
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('null', univ.Null())
            )
        )

        self.s.clear()
        assert encoder.encode(self.s) == ints2octs((48, 0))

    def testInitializedDefaultNullIsNotEncoded(self):
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.DefaultedNamedType('null', univ.Null(''))
            )
        )

        self.s.clear()
        self.s[0] = ''
        assert encoder.encode(self.s) == ints2octs((48, 0))

    def testInitializedOptionalOctetStringIsEncoded(self):
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('str', univ.OctetString())
            )
        )

        self.s.clear()
        self.s[0] = ''
        assert encoder.encode(self.s) == ints2octs((48, 2, 4, 0))

    def testUninitializedOptionalOctetStringIsNotEncoded(self):
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.OptionalNamedType('str', univ.OctetString())
            )
        )

        self.s.clear()
        assert encoder.encode(self.s) == ints2octs((48, 0))

    def testInitializedDefaultOctetStringIsNotEncoded(self):
        self.s = univ.Sequence(
            componentType=namedtype.NamedTypes(
                namedtype.DefaultedNamedType('str', univ.OctetString(''))
            )
        )

        self.s.clear()
        self.s[0] = ''
        assert encoder.encode(self.s) == ints2octs((48, 0))


class ClassConstructorTestCase(BaseTestCase):
    def testKeywords(self):
        tagmap = {"tagmap": True}
        typemap = {"typemap": True}

        sie = encoder.Encoder()._singleItemEncoder
        self.assertIs(sie._tagMap, encoder.TAG_MAP)
        self.assertIs(sie._typeMap, encoder.TYPE_MAP)

        sie = encoder.Encoder(
            tagMap=tagmap, typeMap=typemap
        )._singleItemEncoder
        self.assertIs(sie._tagMap, tagmap)
        self.assertIs(sie._typeMap, typemap)

        sie = encoder.Encoder(tagmap, typemap)._singleItemEncoder
        self.assertIs(sie._tagMap, tagmap)
        self.assertIs(sie._typeMap, typemap)


suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite)
