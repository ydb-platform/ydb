#include "ast.h"

#include <util/generic/stack.h>

#include <utility>

namespace NSQLFormat {

namespace {

bool IsUnstable(const NYql::TAstNode* x) {
    return x->IsAtom() && (x->GetFlags() & NYql::TAstNodeFlags::UnstableFormat) != 0;
}

} // namespace

TMaybe<bool> AreAstEqual(const NYql::TAstNode* lhs, const NYql::TAstNode* rhs) {
    bool isUnstable = false;

    TStack<std::pair<const NYql::TAstNode*, const NYql::TAstNode*>> stack;
    stack.emplace(lhs, rhs);

    while (!stack.empty()) {
        const auto [lhs, rhs] = std::move(stack.top());
        stack.pop();

        if (IsUnstable(lhs) && IsUnstable(rhs)) {
            isUnstable = true;
            continue;
        }

        if (lhs->GetType() != rhs->GetType()) {
            return false;
        }

        switch (lhs->GetType()) {
            case NYql::TAstNode::EType::Atom: {
                if (lhs->GetFlags() != rhs->GetFlags()) {
                    return false;
                }

                if (lhs->GetContent() != rhs->GetContent()) {
                    return false;
                }

                break;
            }
            case NYql::TAstNode::EType::List: {
                if (lhs->GetChildrenCount() != rhs->GetChildrenCount()) {
                    return false;
                }

                for (size_t i = 0; i < lhs->GetChildrenCount(); ++i) {
                    stack.emplace(lhs->GetChild(i), rhs->GetChild(i));
                }

                break;
            }
        }
    }

    if (isUnstable) {
        return Nothing();
    }

    return true;
}

} // namespace NSQLFormat
