#pragma once

#include <util/system/defaults.h>
#include <util/generic/yexception.h>
#include <util/generic/ptr.h>
#include <util/generic/refcount.h>
#include <util/stream/input.h>
#include <util/stream/str.h>

#include "compressor.h"

namespace NCompProto {
    const size_t MAX_ELEMENTS = 32;

    struct TScalarDefaultValue {
        TScalarDefaultValue()
            : Type(None)
            , Value(0)
        {
        }
        enum EType {
            None = 0,
            Fixed = 1,
        };
        EType Type;
        ui32 Value;
    };

    template <class X>
    struct TMetaInfo {
        X Index;
        X Count;
        X Mask;
        X Scalar[MAX_ELEMENTS];
        TAtomicSharedPtr<TMetaInfo> Repeated[MAX_ELEMENTS];
        TScalarDefaultValue Default[MAX_ELEMENTS];

        ui32 ScalarMask;
        ui32 RepeatedMask;
        size_t Size;
        TString Name;
        TString ChildName[MAX_ELEMENTS];
        size_t Id;
        TMetaInfo* Parent;

        struct TCheck {
            ui32 Count;
            ui32 Mask;
            ui32 LastIndex;
            ui32 Check(size_t id, size_t rule) {
                ui32 mask = 1UL << id;
                if (Mask & mask) {
                    ythrow yexception() << "Mask & mask";
                }
                Mask |= mask;
                Y_ENSURE(rule & mask, "!(rule & mask)");
                return mask;
            }
            void ClearCount() {
                Count = 0;
            }
            void ClearMask() {
                Mask = 0;
            }
            void ClearIndex() {
                LastIndex = ui32(-1);
            }
            void ClearAll() {
                ClearCount();
                ClearIndex();
                ClearMask();
            }
            TCheck() {
                ClearAll();
            }
        };

        TCheck Serializer;

        void SetDefaults(TMetaInfo* parent) {
            ScalarMask = 0;
            RepeatedMask = 0;
            Size = 0;
            Id = 0;
            Parent = parent;
        }

        template <class T, class TBuilder>
        TMetaInfo(const TMetaInfo<T>& meta, const TBuilder& builder) {
            SetDefaults(nullptr);
            Name = meta.Name;
            builder.Build(Index, meta.Index);
            builder.Build(Count, meta.Count);
            builder.Build(Mask, meta.Mask);
            Size = meta.Size;
            Id = meta.Id;
            ScalarMask = meta.ScalarMask;
            RepeatedMask = meta.RepeatedMask;
            for (size_t i = 0; i < Size; ++i) {
                ui32 index = (1UL << i);
                if (ScalarMask & index) {
                    builder.Build(Scalar[i], meta.Scalar[i]);
                    ChildName[i] = meta.ChildName[i];
                    Default[i] = meta.Default[i];
                }
                if (RepeatedMask & index) {
                    THolder<TMetaInfo> info(new TMetaInfo(*meta.Repeated[i], builder));
                    info->Parent = this;
                    Repeated[i].Reset(info.Release());
                }
            }
        }

        TMetaInfo(TMetaInfo* parent) {
            SetDefaults(parent);
        }

        template <class T>
        TMetaInfo& BeginRepeated(size_t id, T& functor) {
            Serializer.Check(id, RepeatedMask);
            TMetaInfo& res = *Repeated[id].Get();
            res.Count.BeginElement(functor);
            return res;
        }

        template <class T>
        void BeginSelf(T& functor) {
            Serializer.ClearAll();
            Count.BeginElement(functor);
        }

        template <class T>
        void EndRepeated(T& functor) {
            Count.AddDelayed(Serializer.Count, functor);
            Serializer.ClearCount();
            Serializer.ClearIndex();
            Y_ENSURE(Serializer.Mask == 0, "Serializer.Mask != 0");
        }

        template <class T>
        void BeginElement(ui32 index, T& functor) {
            Y_ENSURE(!(index < Serializer.LastIndex + 1),
                     "Name: " << Name.Quote() << " error: index < Serializer.LastIndex + 1; "
                              << "index: " << index << " LastIndex: " << Serializer.LastIndex);
            Index.Add(index - Serializer.LastIndex, functor);
            Mask.BeginElement(functor);
            Serializer.LastIndex = index;
            ++Serializer.Count;
        }

        template <class TFunctor>
        void Iterate(TFunctor& functor) {
            Cout << Name << " "
                 << "Index" << Endl;
            functor.Process(Index);
            Cout << "Count" << Endl;
            functor.Process(Count);
            Cout << "Mask" << Endl;
            functor.Process(Mask);

            for (size_t i = 0; i < Size; ++i) {
                ui32 index = (1UL << i);
                if (ScalarMask & index) {
                    Cout << "scalar"
                         << " " << i << Endl;
                    functor.Process(Scalar[i]);
                }
                if (RepeatedMask & index) {
                    Cout << "repeated"
                         << " " << i << Endl;
                    Repeated[i]->Iterate(functor);
                }
            }
        }

        template <class T>
        void EndElement(T& functor) {
            Mask.AddDelayed(Serializer.Mask, functor);
            Serializer.ClearMask();
        }

        template <class T>
        void SetScalar(size_t id, ui32 value, T& functor) {
            if (Default[id].Type != TScalarDefaultValue::Fixed || value != Default[id].Value) {
                Serializer.Check(id, ScalarMask);
                Scalar[id].Add(value, functor);
            }
        }

        ui32 Check(size_t id) {
            Y_ENSURE(id < MAX_ELEMENTS, "id >= MAX_ELEMENTS");

            ui32 mask = 1UL << id;
            if (ScalarMask & mask) {
                ythrow yexception() << "ScalarMask & mask";
            }
            if (RepeatedMask & mask) {
                ythrow yexception() << "RepeatedMask & mask";
            }
            Size = Max(id + 1, Size);
            return mask;
        }

        TMetaInfo(IInputStream& stream) {
            SetDefaults(nullptr);
            Load(stream);
        }

        TMetaInfo(const TString& str) {
            SetDefaults(nullptr);
            TStringInput stream(str);
            Load(stream);
        }

        void Save(IOutputStream& stream, const TString& offset = TString()) {
            stream << offset << "repeated " << Name << " id " << Id << Endl;
            TString step = "    ";
            stream << step << offset << "index" << Endl;
            Index.Save(stream, step + offset);
            stream << step << offset << "count" << Endl;
            Count.Save(stream, step + offset);
            stream << step << offset << "mask" << Endl;
            Mask.Save(stream, step + offset);

            for (size_t i = 0; i < MAX_ELEMENTS; ++i) {
                ui32 mask = 1UL << i;
                if (mask & RepeatedMask) {
                    Repeated[i]->Save(stream, step + offset);
                } else if (mask & ScalarMask) {
                    stream << step << offset << "scalar " << ChildName[i] << " id " << i;
                    stream << " default " << (Default[i].Type == TScalarDefaultValue::None ? " no " : " const ");
                    if (Default[i].Type == TScalarDefaultValue::Fixed) {
                        stream << Default[i].Value;
                    }
                    stream << Endl;
                    stream << step << offset << "table "
                           << "id " << i << Endl;
                    Scalar[i].Save(stream, step + offset);
                }
            }
            stream << offset << "end" << Endl;
        }

        void Load(IInputStream& stream) {
            TString name;
            stream >> name;
            if (name == "repeated") {
                stream >> name;
            }
            Name = name;
            stream >> name;
            Y_ENSURE(name == "id", "Name mismatch: " << name.Quote() << " != id. ");
            stream >> Id;

            while (1) {
                stream >> name;
                if (name == "index") {
                    Index.Load(stream);
                } else if (name == "count") {
                    Count.Load(stream);
                } else if (name == "mask") {
                    Mask.Load(stream);
                } else if (name == "table") {
                    stream >> name;
                    Y_ENSURE(name == "id", "Name mismatch: " << name.Quote() << " != id. ");
                    size_t id;
                    stream >> id;
                    Scalar[id].Load(stream);
                } else if (name == "repeated") {
                    THolder<TMetaInfo> info(new TMetaInfo(this));
                    info->Load(stream);
                    size_t id = info->Id;
                    RepeatedMask |= Check(id);
                    Repeated[id].Reset(info.Release());
                } else if (name == "scalar") {
                    stream >> name;
                    TString childName = name;
                    stream >> name;
                    Y_ENSURE(name == "id", "Name mismatch: " << name.Quote() << " != id. ");
                    size_t id;
                    stream >> id;
                    ScalarMask |= Check(id);
                    ChildName[id] = childName;
                    stream >> name;
                    Y_ENSURE(name == "default", "Name mismatch: " << name.Quote() << " != default. ");
                    stream >> name;
                    if (name == "no") {
                        Default[id].Type = TScalarDefaultValue::None;
                    } else if (name == "const") {
                        ui32 def;
                        stream >> def;
                        Default[id].Type = TScalarDefaultValue::Fixed;
                        Default[id].Value = def;
                    } else {
                        ythrow yexception() << "Unsupported default value specification: " << name.Quote();
                    }
                } else if (name == "end" /* || stream.IsEOF()*/) {
                    return;
                }
            }
        }
    };

}
