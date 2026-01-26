#include "rpc_fs_path_validation.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NGRpcService {

Y_UNIT_TEST_SUITE(ValidateFsPathTests) {
    Y_UNIT_TEST(EmptyPath) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(SimplePath) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/mnt/data/backup", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(PathTraversalAtStart) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("../etc/passwd", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(PathTraversalInMiddle) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("/mnt/../etc/passwd", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(PathTraversalAtEnd) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("/mnt/data/..", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(MultiplePathTraversals) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("../../..", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(CurrentDirectoryAtStart) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("./data", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "current directory reference");
    }

    Y_UNIT_TEST(CurrentDirectoryInMiddle) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("/mnt/./data", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "current directory reference");
    }

    Y_UNIT_TEST(CurrentDirectoryAtEnd) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("/mnt/data/.", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "current directory reference");
    }

    Y_UNIT_TEST(MultipleSlashes) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/mnt//data///backup", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(TrailingSlash) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/mnt/data/", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(RelativePathWithoutDots) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("backup/data", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(ComponentContainingDots) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/mnt/file..txt", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(ComponentStartingWithDots) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/mnt/...hidden", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(ErrorMessageContainsPathDescription) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("/mnt/..", "base_path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "base_path");
    }

    Y_UNIT_TEST(ComplexValidPath) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/mnt/data/backup/2024/01/15/table_data.csv", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(PathWithSpecialCharacters) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/mnt/data-backup/table_v2.0", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(RepeatedTraversalAttempt) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("/mnt/../../../../../../../etc/passwd", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(HiddenFile) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/home/user/.bashrc", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(DotInFilename) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("/mnt/data.tar.gz", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(NullByteRejection) {
        TString error;
        TString pathWithNull = TString("/mnt/data") + TString('\0') + TString("file");
        UNIT_ASSERT(!ValidateFsPath(pathWithNull, "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "null byte");
    }

    Y_UNIT_TEST(NullByteInMiddle) {
        TString error;
        TString pathWithNull = TString("normal/path\0/../etc", 20);
        UNIT_ASSERT(!ValidateFsPath(pathWithNull, "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "null byte");
    }

    Y_UNIT_TEST(PathTraversalCombinations) {
        TString error;

        UNIT_ASSERT(!ValidateFsPath("..\\..\\..\\etc\\passwd", "test path", error));
#ifdef _unix_
        UNIT_ASSERT_STRING_CONTAINS(error, "backslash");
#else
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
#endif

        error.clear();
        UNIT_ASSERT(!ValidateFsPath("/var/../../etc/shadow", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");

        error.clear();
        UNIT_ASSERT(!ValidateFsPath("backup/../../../root", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(CurrentDirectoryVariants) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("./backup", "test path", error));

        error.clear();
        UNIT_ASSERT(!ValidateFsPath("backup/./data", "test path", error));

        error.clear();
        UNIT_ASSERT(!ValidateFsPath("backup/data/.", "test path", error));
    }

#ifdef _unix_
    Y_UNIT_TEST(UnixRejectsBackslash) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("/mnt\\data", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "backslash");
    }

    Y_UNIT_TEST(UnixRejectsWindowsStyleTraversal) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("\\Users\\..\\Windows", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "backslash");
    }
#endif

#ifdef _win_
    Y_UNIT_TEST(WindowsPathWithBackslashes) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("C:\\Users\\data", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(WindowsPathWithForwardSlashes) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("C:/Users/data", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(WindowsPathTraversalWithBackslashes) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("C:\\Users\\..\\Windows", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(WindowsPathTraversalWithForwardSlashes) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("C:/Users/../Windows", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(WindowsUNCPath) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("\\\\server\\share\\data", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(WindowsUNCPathWithTraversal) {
        TString error;
        UNIT_ASSERT(!ValidateFsPath("\\\\server\\share\\..\\other", "test path", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "path traversal");
    }

    Y_UNIT_TEST(WindowsDriveLetterOnly) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("C:", "test path", error));
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(WindowsDriveLetterWithBackslash) {
        TString error;
        UNIT_ASSERT(ValidateFsPath("C:\\", "test path", error));
        UNIT_ASSERT(error.empty());
    }
#endif
}

} // namespace NGRpcService
} // namespace NKikimr
