#include "purecalc.h"

namespace NKikimr::NReplication::NTransfer {

namespace {

class TProgramHolder : public IProgramHolder {
public:
    using TPtr = TIntrusivePtr<TProgramHolder>;

public:
    TProgramHolder(
        const TScheme::TPtr& tableScheme,
        const TString& sql
    )
        : TopicColumns()
        , TableScheme(tableScheme)
        , Sql(sql)
    {}

public:
    void CreateProgram(NYql::NPureCalc::IProgramFactoryPtr programFactory) override {
        // Program should be stateless because input values
        // allocated on another allocator and should be released
        Program = programFactory->MakePullListProgram(
            TMessageInputSpec(),
            TMessageOutputSpec(TableScheme, MakeOutputSchema(TableScheme->TableColumns)),
            Sql,
            NYql::NPureCalc::ETranslationMode::SQL
        );
    }

    NYql::NPureCalc::TPullListProgram<TMessageInputSpec, TMessageOutputSpec>* GetProgram() override {
        return Program.Get();
    }

private:
    const TVector<TSchemeColumn> TopicColumns;
    const TScheme::TPtr TableScheme;
    const TString Sql;

    THolder<NYql::NPureCalc::TPullListProgram<TMessageInputSpec, TMessageOutputSpec>> Program;
};

}

IProgramHolder::TPtr CreateProgramHolder(const TScheme::TPtr& tableScheme, const TString& sql) {
    return MakeIntrusive<TProgramHolder>(tableScheme, sql);
}

}
