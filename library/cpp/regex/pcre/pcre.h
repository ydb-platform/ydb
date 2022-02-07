#pragma once

#include "traits.h"

#include <library/cpp/containers/stack_array/stack_array.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NPcre {
    //! Start and end offset for match group.
    using TPcreMatch = std::pair<int, int>;

    //! Full match result containing all capturing groups.
    /*!
     *  At zero index we have whole matched string start and end offsets.
     *  All other elements will contain capturing groups positions.
     *  Non-captured capturing groups will have {-1, -1} offsets.
     */
    using TPcreMatches = TVector<TPcreMatch>;

    //! Compiled pattern optimization strategy.
    enum class EOptimize {
        //! No optimization.
        /*!
         *  Useful for non-reusable patterns where compile time matters.
         */
        None,
        //! Basic optimization via |pcre_study|.
        /*!
         *  Could give up to 4x match speed boost in exchange of increased
         *  construction time. Could not.
         */
        Study,
        //! PCRE JIT optimization.
        /*!
         *  Could give up to 10x match speed bust in exchange of significantly
         *  increased compile time. Also, for very complex patterns |pcre_exec|
         *  could return |PCRE_ERROR_JIT_STACKLIMIT|. See
         *  https://www.pcre.org/original/doc/html/pcrejit.html for details.
         */
        JIT
    };

    //! PCRE code container. Controls its life time and provides handy wrapper.
    template <class TCharType>
    class TPcre {
    private:
        using TCodeType = typename TPcreTraits<TCharType>::TCodeType;
        using TExtraType = typename TPcreTraits<TCharType>::TExtraType;
        using TStringType = typename TPcreTraits<TCharType>::TStringType;
        using TTraits = TPcreTraits<TCharType>;
        static constexpr size_t DefaultWorkspaceSize = 16;

    public:
        //! Compiles regexp into internal representation for future use.
        /*!
         *  \param pattern      Regular expression to be compiled.
         *  \param optimize     If |EOptimize::JIT|, perform additional
         *                      analysis, which will take extra time, but could
         *                      speed up matching. |None| to omit optimization.
         *  \param compileFlags See https://www.pcre.org/original/doc/html/pcre_compile2.html
         **/
        TPcre(const TCharType* pattern, EOptimize optimize = EOptimize::None, int compileFlags = 0) {
            int errcode;
            const char* errptr;
            int erroffset;
            Code.Reset(TTraits::Compile((TStringType) pattern, compileFlags, &errcode, &errptr, &erroffset, nullptr));
            if (!Code) {
                ythrow yexception() << "Failed to compile pattern <" << pattern
                    << ">, because of error at pos " << erroffset
                    << ", error code " << errcode << ": " << errptr;
            }
            if (optimize != EOptimize::None) {
                errptr = nullptr;
                int options;
                if (optimize == EOptimize::Study) {
                    options = 0;
                } else {
                    options = PCRE_STUDY_JIT_COMPILE;
                }
                Extra.Reset(TTraits::Study(Code.Get(), options, &errptr));
                if (errptr) {
                    ythrow yexception() << "Failed to study pattern <" << pattern << ">: " << errptr;
                }
            }
        }

        //! Check if compiled pattern matches string.
        /*!
         *  \param string           String to search in.
         *  \param executeFlags     See https://www.pcre.org/original/doc/html/pcre_exec.html
         *  \param workspaceSize    Amount of space which will be allocated for
         *                          back references. PCRE could allocate more
         *                          heap space is provided workspaceSize won't
         *                          fit all of them.
         *  \returns                |true| if there is a match.
         */
        bool Matches(TBasicStringBuf<TCharType> string, int executeFlags = 0, size_t workspaceSize = DefaultWorkspaceSize) const {
            Y_ASSERT(workspaceSize >= 0);
            size_t ovecsize = workspaceSize * 3;
            NStackArray::TStackArray<int> ovector(ALLOC_ON_STACK(int, ovecsize));
            return ConvertReturnCode(TTraits::Exec(Code.Get(), Extra.Get(), (TStringType) string.Data(), string.Size(), 0, executeFlags, ovector.data(), ovecsize));
        }

        //! Find compiled pattern in string.
        /*!
         *  \param string           String to search in.
         *  \param executeFlags     See https://www.pcre.org/original/doc/html/pcre_exec.html
         *  \param workspaceSize    Amount of space which will be allocated for
         *                          back references. PCRE could allocate more
         *                          heap space is provided workspaceSize won't
         *                          fit all of them.
         *  \returns                Start and end offsets pair if there is a
         *                          match. |Nothing| otherwise.
         */
        Y_NO_SANITIZE("memory") TMaybe<TPcreMatch> Find(TBasicStringBuf<TCharType> string, int executeFlags = 0, size_t workspaceSize = DefaultWorkspaceSize) const {
            Y_ASSERT(workspaceSize >= 0);
            size_t ovecsize = workspaceSize * 3;
            NStackArray::TStackArray<int> ovector(ALLOC_ON_STACK(int, ovecsize));
            for (size_t i = 0; i < ovecsize; ++i) {
                ovector[i] = -4;
            }
            int rc = TTraits::Exec(Code.Get(), Extra.Get(), (TStringType) string.Data(), string.Size(), 0, executeFlags, ovector.data(), ovecsize);
            if (ConvertReturnCode(rc)) {
                return MakeMaybe<TPcreMatch>(ovector[0], ovector[1]);
            } else {
                return Nothing();
            }
        }

        //! Find and return all capturing groups in string.
        /*!
         *  \param string               String to search in.
         *  \param executeFlags         See https://www.pcre.org/original/doc/html/pcre_exec.html
         *  \param initialWorkspaceSize Capturing groups vector initial size.
         *                              Workspace will be grown and search will
         *                              be repeated if there is not enough
         *                              space.
         *  \returns                    List of capturing groups start and end
         *                              offsets. First element will contain
         *                              whole matched substring start and end
         *                              offsets. For non-matched capturing
         *                              groups, result will contain {-1, -1}
         *                              pair.
         *                              If pattern not found in string, result
         *                              vector will be empty.
         */
        Y_NO_SANITIZE("memory") TPcreMatches Capture(TBasicStringBuf<TCharType> string, int executeFlags = 0, size_t initialWorkspaceSize = DefaultWorkspaceSize) const {
            Y_ASSERT(initialWorkspaceSize > 0);
            size_t ovecsize = (initialWorkspaceSize + 1) * 3;
            while (true) {
                NStackArray::TStackArray<int> ovector(ALLOC_ON_STACK(int, ovecsize));
                int rc = TTraits::Exec(Code.Get(), Extra.Get(), (TStringType) string.Data(), string.Size(), 0, executeFlags, ovector.data(), ovecsize);
                if (rc > 0) {
                    TPcreMatches result(Reserve(rc >> 1));
                    for (int i = 0, pos = 0; i < rc; ++i) {
                        int start = ovector[pos++];
                        int end = ovector[pos++];
                        result.emplace_back(start, end);
                    }
                    return result;
                } else if (rc == 0) {
                    ovecsize <<= 1;
                } else if (rc == PCRE_ERROR_NOMATCH) {
                    return TPcreMatches{};
                } else if (rc < 0) {
                    ythrow yexception() << "Error. RC = " << rc;
                }
            }
        }

    private:
        TPcreCode<TCharType> Code;
        TPcreExtra<TCharType> Extra;

    private:
        static inline bool ConvertReturnCode(int rc) {
            if (rc >= 0) {
                return true;
            } else if (rc == PCRE_ERROR_NOMATCH) {
                return false;
            } else {
                ythrow yexception() << "Error. RC = " << rc;
            }
        }
    };
}

