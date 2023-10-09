#pragma once

#include "control.h"
#include "probe.h"

#include <ctype.h>

namespace NLWTrace {
#ifndef LWTRACE_DISABLE

    // Box that holds dynamically created probe
    // NOTE: must be allocated on heap
    template <LWTRACE_TEMPLATE_PARAMS>
    class TLWProbe: public IBox, public TUserProbe<LWTRACE_TEMPLATE_ARGS> {
    private:
        // Storage for strings referenced by TEvent
        TString Name;
        TString Provider;
        TVector<TString> Groups;
        TVector<TString> Params;
        TVector<TProbeRegistry*> Registries; // Note that we assume that registry lives longer than probe

    public:
        TLWProbe(const TString& provider, const TString& name, const TVector<TString>& groups, const TVector<TString>& params)
            : IBox(true)
            , Name(name)
            , Provider(provider)
            , Groups(groups)
            , Params(params)
        {
            // initialize TProbe
            TProbe& probe = this->Probe;
            probe.Init();

            // initialize TEvent
            Y_ABORT_UNLESS(IsCppIdentifier(Name), "probe '%s' is not C++ identifier", Name.data());
            Y_ABORT_UNLESS(IsCppIdentifier(Provider), "provider '%s' is not C++ identifier in probe %s", Provider.data(), Name.data());
            probe.Event.Name = Name.c_str();
            Zero(probe.Event.Groups);
            probe.Event.Groups[0] = Provider.c_str();
            auto i = Groups.begin(), ie = Groups.end();
            Y_ABORT_UNLESS(Groups.size() < LWTRACE_MAX_GROUPS, "too many groups in probe %s", Name.data());
            for (size_t n = 1; n < LWTRACE_MAX_GROUPS && i != ie; n++, ++i) {
                Y_ABORT_UNLESS(IsCppIdentifier(*i), "group '%s' is not C++ identifier in probe %s", i->data(), Name.data());
                probe.Event.Groups[n] = i->c_str();
            }

            // initialize TSignature
            using TUsrSign = TUserSignature<LWTRACE_TEMPLATE_ARGS>;
            Y_ABORT_UNLESS(TUsrSign::ParamCount == (int)Params.size(), "param count mismatch in probe %s: %d != %d",
                     Name.data(), int(Params.size()), TUsrSign::ParamCount);
            TSignature& signature = probe.Event.Signature;
            signature.ParamTypes = TUsrSign::ParamTypes;
            Zero(signature.ParamNames);
            auto j = Params.begin(), je = Params.end();
            for (size_t n = 0; n < LWTRACE_MAX_PARAMS && j != je; n++, ++j) {
                Y_ABORT_UNLESS(IsCppIdentifier(*j), "param '%s' is not C++ identifier in probe %s", j->data(), Name.data());
                signature.ParamNames[n] = j->c_str();
            }
            signature.ParamCount = TUsrSign::ParamCount;
            signature.SerializeParamsFunc = &TUsrSign::SerializeParams;
            signature.CloneParamsFunc = &TUsrSign::CloneParams;
            signature.DestroyParamsFunc = &TUsrSign::DestroyParams;
            signature.SerializeToPbFunc = &TUsrSign::SerializeToPb;
            signature.DeserializeFromPbFunc = &TUsrSign::DeserializeFromPb;

            // register probe in global registry
            Register(*Singleton<NLWTrace::TProbeRegistry>());
        }

        ~TLWProbe() {
            Unregister();
        }

        void Register(TProbeRegistry& reg) {
            Registries.push_back(&reg);
            reg.AddProbe(TBoxPtr(this)); // NOTE: implied `this' object is created with new operator
        }

        void Unregister() {
            // TODO[serxa]: make sure registry never dies before probe it contain
            // TODO[serxa]: make sure probe never dies before TSession that uses it
            for (TProbeRegistry* reg : Registries) {
                reg->RemoveProbe(&this->Probe);
            }
        }

        TProbe* GetProbe() override {
            return &this->Probe;
        }

    private:
        static bool IsCppIdentifier(const TString& str) {
            bool first = true;
            for (char c : str) {
                if (first) {
                    first = false;
                    if (!(isalpha(c) || c == '_')) {
                        return false;
                    }
                } else if (!(isalnum(c) || c == '_')) {
                    return false;
                }
            }
            return true;
        }
    };

#endif
}
