#include "simple_reflection.h"
#include <library/cpp/protobuf/util/ut/sample_for_simple_reflection.pb.h>
#include <library/cpp/protobuf/util/ut/extensions.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NProtoBuf;

Y_UNIT_TEST_SUITE(ProtobufSimpleReflection) {
    static TSample GenSampleForMergeFrom() {
        TSample smf;
        smf.SetOneStr("one str");
        smf.MutableOneMsg()->AddRepInt(1);
        smf.AddRepMsg()->AddRepInt(2);
        smf.AddRepMsg()->AddRepInt(3);
        smf.AddRepStr("one rep str");
        smf.AddRepStr("two rep str");
        smf.SetAnotherOneStr("another one str");
        return smf;
    }

    Y_UNIT_TEST(MergeFromGeneric) {
        const TSample src(GenSampleForMergeFrom());
        TSample dst;
        const Descriptor* descr = dst.GetDescriptor();

        {
            TMutableField dstOneStr(dst, descr->FindFieldByName("OneStr"));
            TConstField srcOneStr(src, descr->FindFieldByName("OneStr"));
            dstOneStr.MergeFrom(srcOneStr);
            UNIT_ASSERT_VALUES_EQUAL(dst.GetOneStr(), src.GetOneStr());
        }

        { // MergeFrom for single message fields acts like a Message::MergeFrom
            TMutableField dstOneMsg(dst, descr->FindFieldByName("OneMsg"));
            dstOneMsg.MergeFrom(TConstField(src, descr->FindFieldByName("OneMsg")));
            UNIT_ASSERT_VALUES_EQUAL(dst.GetOneMsg().RepIntSize(), src.GetOneMsg().RepIntSize());
            dstOneMsg.MergeFrom(TConstField(src, descr->FindFieldByName("OneMsg")));
            UNIT_ASSERT_VALUES_EQUAL(dst.GetOneMsg().RepIntSize(), src.GetOneMsg().RepIntSize() * 2);
        }

        { // MergeFrom for repeated fields acts like append
            TMutableField dstRepMsg(dst, descr->FindFieldByName("RepMsg"));
            dstRepMsg.MergeFrom(TConstField(src, descr->FindFieldByName("RepMsg")));
            UNIT_ASSERT_VALUES_EQUAL(dst.RepMsgSize(), src.RepMsgSize());
            dstRepMsg.MergeFrom(TConstField(src, descr->FindFieldByName("RepMsg")));
            UNIT_ASSERT_VALUES_EQUAL(dst.RepMsgSize(), src.RepMsgSize() * 2);
            for (size_t repMsgIndex = 0; repMsgIndex < dst.RepMsgSize(); ++repMsgIndex) {
                UNIT_ASSERT_VALUES_EQUAL(dst.GetRepMsg(repMsgIndex).RepIntSize(), src.GetRepMsg(0).RepIntSize());
            }
        }
    }

    Y_UNIT_TEST(MergeFromSelf) {
        const TSample sample(GenSampleForMergeFrom());
        TSample msg(sample);
        const Descriptor* descr = msg.GetDescriptor();

        TMutableField oneStr(msg, descr->FindFieldByName("OneStr"));
        oneStr.MergeFrom(oneStr);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetOneStr(), sample.GetOneStr());

        TMutableField oneMsg(msg, descr->FindFieldByName("OneMsg"));
        oneMsg.MergeFrom(oneMsg); // nothing should change
        UNIT_ASSERT_VALUES_EQUAL(msg.GetOneMsg().RepIntSize(), sample.GetOneMsg().RepIntSize());
    }

    Y_UNIT_TEST(MergeFromAnotherFD) {
        const TSample sample(GenSampleForMergeFrom());
        TSample msg(GenSampleForMergeFrom());
        const Descriptor* descr = msg.GetDescriptor();

        { // string
            TMutableField oneStr(msg, descr->FindFieldByName("OneStr"));
            TMutableField repStr(msg, descr->FindFieldByName("RepStr"));
            TMutableField anotherOneStr(msg, descr->FindFieldByName("AnotherOneStr"));
            oneStr.MergeFrom(anotherOneStr);
            UNIT_ASSERT_VALUES_EQUAL(msg.GetOneStr(), sample.GetAnotherOneStr());
            oneStr.MergeFrom(repStr);
            const size_t sampleRepStrSize = sample.RepStrSize();
            UNIT_ASSERT_VALUES_EQUAL(msg.GetOneStr(), sample.GetRepStr(sampleRepStrSize - 1));
            repStr.MergeFrom(anotherOneStr);
            UNIT_ASSERT_VALUES_EQUAL(msg.RepStrSize(), sampleRepStrSize + 1);
            UNIT_ASSERT_VALUES_EQUAL(msg.GetRepStr(sampleRepStrSize), msg.GetAnotherOneStr());
        }

        { // Message
            TMutableField oneMsg(msg, descr->FindFieldByName("OneMsg"));
            TMutableField repMsg(msg, descr->FindFieldByName("RepMsg"));
            oneMsg.MergeFrom(repMsg);
            const size_t oneMsgRepIntSize = sample.GetOneMsg().RepIntSize();
            const size_t sizeOfAllRepIntsInRepMsg = sample.RepMsgSize();
            UNIT_ASSERT_VALUES_EQUAL(msg.GetOneMsg().RepIntSize(), oneMsgRepIntSize + sizeOfAllRepIntsInRepMsg);
            repMsg.MergeFrom(oneMsg);
            UNIT_ASSERT_VALUES_EQUAL(msg.RepMsgSize(), sample.RepMsgSize() + 1);
        }
    }

    Y_UNIT_TEST(RemoveByIndex) {
        TSample msg;

        const Descriptor* descr = msg.GetDescriptor();
        {
            TMutableField fld(msg, descr->FindFieldByName("RepMsg"));
            msg.AddRepMsg()->AddRepInt(1);
            msg.AddRepMsg()->AddRepInt(2);
            msg.AddRepMsg()->AddRepInt(3);

            UNIT_ASSERT_VALUES_EQUAL(3, msg.RepMsgSize()); // 1, 2, 3
            fld.Remove(1);                                 // from middle
            UNIT_ASSERT_VALUES_EQUAL(2, msg.RepMsgSize());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.GetRepMsg(0).GetRepInt(0));
            UNIT_ASSERT_VALUES_EQUAL(3, msg.GetRepMsg(1).GetRepInt(0));

            msg.AddRepMsg()->AddRepInt(5);
            UNIT_ASSERT_VALUES_EQUAL(3, msg.RepMsgSize()); // 1, 3, 5
            fld.Remove(2);                                 // from end
            UNIT_ASSERT_VALUES_EQUAL(2, msg.RepMsgSize());
            UNIT_ASSERT_VALUES_EQUAL(1, msg.GetRepMsg(0).GetRepInt(0));
            UNIT_ASSERT_VALUES_EQUAL(3, msg.GetRepMsg(1).GetRepInt(0));
            msg.ClearRepMsg();
        }

        {
            TMutableField fld(msg, descr->FindFieldByName("RepStr"));
            msg.AddRepStr("1");
            msg.AddRepStr("2");
            msg.AddRepStr("3");
            UNIT_ASSERT_VALUES_EQUAL(3, msg.RepStrSize()); // "1", "2", "3"
            fld.Remove(0);                                 // from begin
            UNIT_ASSERT_VALUES_EQUAL(2, msg.RepStrSize());
            UNIT_ASSERT_VALUES_EQUAL("2", msg.GetRepStr(0));
            UNIT_ASSERT_VALUES_EQUAL("3", msg.GetRepStr(1));
        }

        {
            TMutableField fld(msg, descr->FindFieldByName("OneStr"));
            msg.SetOneStr("1");
            UNIT_ASSERT(msg.HasOneStr());
            fld.Remove(0); // not repeated
            UNIT_ASSERT(!msg.HasOneStr());
        }
    }

    Y_UNIT_TEST(GetFieldByPath) {
        // Simple get by path
        {
            TSample msg;
            msg.SetOneStr("1");
            msg.MutableOneMsg()->AddRepInt(2);
            msg.MutableOneMsg()->AddRepInt(3);
            msg.AddRepMsg()->AddRepInt(4);
            msg.MutableRepMsg(0)->AddRepInt(5);
            msg.AddRepMsg()->AddRepInt(6);

            {
                TMaybe<TConstField> field = TConstField::ByPath(msg, "OneStr");
                UNIT_ASSERT(field);
                UNIT_ASSERT(field->HasValue());
                UNIT_ASSERT_VALUES_EQUAL("1", (field->Get<TString>()));
            }

            {
                TMaybe<TConstField> field = TConstField::ByPath(msg, "OneMsg");
                UNIT_ASSERT(field);
                UNIT_ASSERT(field->HasValue());
                UNIT_ASSERT(field->IsMessageInstance<TInnerSample>());
            }

            {
                TMaybe<TConstField> field = TConstField::ByPath(msg, "/OneMsg/RepInt");
                UNIT_ASSERT(field);
                UNIT_ASSERT(field->HasValue());
                UNIT_ASSERT_VALUES_EQUAL(2, field->Size());
                UNIT_ASSERT_VALUES_EQUAL(2, field->Get<int>(0));
                UNIT_ASSERT_VALUES_EQUAL(3, field->Get<int>(1));
            }

            {
                TMaybe<TConstField> field = TConstField::ByPath(msg, "RepMsg/RepInt");
                UNIT_ASSERT(field);
                UNIT_ASSERT(field->HasValue());
                UNIT_ASSERT_VALUES_EQUAL(2, field->Size());
                UNIT_ASSERT_VALUES_EQUAL(4, field->Get<int>(0));
                UNIT_ASSERT_VALUES_EQUAL(5, field->Get<int>(1));
            }
        }

        // get of unset fields
        {
            TSample msg;
            msg.MutableOneMsg();

            {
                TMaybe<TConstField> field = TConstField::ByPath(msg, "OneStr");
                UNIT_ASSERT(field);
                UNIT_ASSERT(!field->HasValue());
            }

            {
                TMaybe<TConstField> field = TConstField::ByPath(msg, "OneMsg/RepInt");
                UNIT_ASSERT(field);
                UNIT_ASSERT(!field->HasValue());
            }

            {
                TMaybe<TConstField> field = TConstField::ByPath(msg, "RepMsg/RepInt");
                UNIT_ASSERT(!field);
            }
        }

        // mutable
        {
            TSample msg;
            msg.MutableOneMsg();

            {
                TMaybe<TMutableField> field = TMutableField::ByPath(msg, "OneStr");
                UNIT_ASSERT(field);
                UNIT_ASSERT(!field->HasValue());
                field->Set(TString("zz"));
                UNIT_ASSERT(field->HasValue());
                UNIT_ASSERT_VALUES_EQUAL("zz", msg.GetOneStr());
            }

            {
                TMaybe<TMutableField> field = TMutableField::ByPath(msg, "OneStr");
                UNIT_ASSERT(field);
                UNIT_ASSERT(field->HasValue());
                field->Set(TString("dd"));
                UNIT_ASSERT(field->HasValue());
                UNIT_ASSERT_VALUES_EQUAL("dd", msg.GetOneStr());
            }

            {
                TMaybe<TMutableField> field = TMutableField::ByPath(msg, "OneMsg/RepInt");
                UNIT_ASSERT(field);
                UNIT_ASSERT(!field->HasValue());
                field->Add(10);
                UNIT_ASSERT_VALUES_EQUAL(10, msg.GetOneMsg().GetRepInt(0));
            }

            {
                TMaybe<TMutableField> field = TMutableField::ByPath(msg, "RepMsg/RepInt");
                UNIT_ASSERT(!field);
            }
        }

        // mutable with path creation
        {
            TSample msg;

            {
                TMaybe<TMutableField> field = TMutableField::ByPath(msg, "OneStr", true);
                UNIT_ASSERT(field);
                UNIT_ASSERT(!field->HasValue());
            }

            {
                TMaybe<TMutableField> field = TMutableField::ByPath(msg, "OneMsg/RepInt", true);
                UNIT_ASSERT(field);
                UNIT_ASSERT(!field->HasValue());
                UNIT_ASSERT(msg.HasOneMsg());
                field->Add(10);
                UNIT_ASSERT_VALUES_EQUAL(10, msg.GetOneMsg().GetRepInt(0));
            }

            {
                TMaybe<TMutableField> field = TMutableField::ByPath(msg, "RepMsg/RepInt", true);
                TMaybe<TMutableField> fieldCopy = TMutableField::ByPath(msg, "RepMsg/RepInt", true);
                Y_UNUSED(fieldCopy);
                UNIT_ASSERT(field);
                UNIT_ASSERT(!field->HasValue());
                UNIT_ASSERT_VALUES_EQUAL(1, msg.RepMsgSize());
                field->Add(12);
                UNIT_ASSERT_VALUES_EQUAL(12, field->Get<int>());
            }
        }

        // error
        {
            {TSample msg;
        UNIT_ASSERT(!TConstField::ByPath(msg, "SomeField"));
    }

    {
        TSample msg;
        UNIT_ASSERT(!TMutableField::ByPath(msg, "SomeField/FieldSome"));
    }

    {
        TSample msg;
        UNIT_ASSERT(!TMutableField::ByPath(msg, "SomeField/FieldSome", true));
    }
}

// extension
{
    TSample msg;
    msg.SetExtension(NExt::TTestExt::ExtField, "ext");
    msg.SetExtension(NExt::ExtField, 2);
    msg.AddExtension(NExt::Ext2Field, 33);
    TInnerSample* subMsg = msg.MutableExtension(NExt::SubMsgExt);
    subMsg->AddRepInt(20);
    subMsg->SetExtension(NExt::Ext3Field, 54);

    {
        TMaybe<TConstField> field = TConstField::ByPath(msg, "NExt.TTestExt.ExtField");
        UNIT_ASSERT(field);
        UNIT_ASSERT(field->HasValue());
        UNIT_ASSERT_VALUES_EQUAL("ext", field->Get<TString>());
    }
    {
        TMaybe<TConstField> field = TConstField::ByPath(msg, "NExt.ExtField");
        UNIT_ASSERT(field);
        UNIT_ASSERT(field->HasValue());
        UNIT_ASSERT_VALUES_EQUAL(2, field->Get<int>());
    }
    {
        TMaybe<TConstField> field = TConstField::ByPath(msg, "ExtField"); // ambiguity
        UNIT_ASSERT(!field);
    }
    {
        TMaybe<TConstField> field = TConstField::ByPath(msg, "NExt.Ext2Field");
        UNIT_ASSERT(field);
        UNIT_ASSERT(field->HasValue());
        UNIT_ASSERT_VALUES_EQUAL(33, field->Get<int>());
    }
    {
        TMaybe<TConstField> field = TConstField::ByPath(msg, "Ext2Field");
        UNIT_ASSERT(field);
        UNIT_ASSERT(field->HasValue());
        UNIT_ASSERT_VALUES_EQUAL(33, field->Get<int>());
    }
    {
        TMaybe<TConstField> field = TConstField::ByPath(msg, "SubMsgExt");
        UNIT_ASSERT(field);
        UNIT_ASSERT(field->HasValue());
        const TInnerSample* subMsg2 = field->GetAs<TInnerSample>();
        UNIT_ASSERT(subMsg2);
        UNIT_ASSERT_VALUES_EQUAL(1, subMsg2->RepIntSize());
        UNIT_ASSERT_VALUES_EQUAL(20, subMsg2->GetRepInt(0));
        UNIT_ASSERT_VALUES_EQUAL(54, subMsg2->GetExtension(NExt::Ext3Field));
    }
    {
        TMaybe<TConstField> field = TConstField::ByPath(msg, "SubMsgExt/Ext3Field");
        UNIT_ASSERT(field);
        UNIT_ASSERT(field->HasValue());
        UNIT_ASSERT_VALUES_EQUAL(54, field->Get<int>());
    }
    {
        TMaybe<TConstField> field = TConstField::ByPath(msg, "SubMsgExt/RepInt");
        UNIT_ASSERT(field);
        UNIT_ASSERT(field->HasValue());
        UNIT_ASSERT_VALUES_EQUAL(20, field->Get<int>());
    }
}
}
}
