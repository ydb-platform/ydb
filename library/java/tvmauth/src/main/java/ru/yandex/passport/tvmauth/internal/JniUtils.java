/**
 * Some similar logic already exists in Arcadia:
 *   https://a.yandex-team.ru/arc/trunk/arcadia/iceberg/misc/src/main/java/ru/yandex/misc/jni/JniUtils.java
 * or in github:
 *   https://github.com/adamheinrich/native-utils/blob/master/src/main/java/cz/adamh/utils/NativeUtils.java
 *
 * This class doesn't use side utils and contains all logic inside
 *   to provide hermetic jar for exporting out from Arcadia.
 */

package ru.yandex.passport.tvmauth.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniUtils {
    private static final Logger logger = LoggerFactory.getLogger(JniUtils.class);
    private static boolean wasCalled = false;

    private JniUtils() {
    }

    /**
     * Loads library from .jar by default.
     * NOTE: this method will be called implicitly
     */
    public static synchronized void loadLibrary() {
        if (wasCalled) {
            return;
        }

        try {
            tryLoadFromJar();
            wasCalled = true;
        } catch (RuntimeException e) {
            logger.error("Failed to load native library from jar: {}", e);
            throw e;
        } catch (IOException e) {
            logger.error("Failed to load native library from jar: {}", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads library from java.library.path
     *
     * WARNING! If you use this method directly, you make the commitment:
     *   it is your responsibility to keep .jar and native library (.so/.dll/.dylib)
     *   in the same revision!
     */
    public static synchronized void loadLibraryFromLibraryPath() {
        loadInLegacyWay();
        wasCalled = true;
    }

    private static void loadInLegacyWay() {
        System.loadLibrary("tvmauth_java");
    }

    private static void tryLoadFromJar() throws IOException {
        // Run with arm64 from maven
        if ("aarch64".equals(System.getProperty("os.arch"))) {
            if (tryLoadResource("tvmauth_java_arm64")) {
                return;
            }
        }

        // Any run from Arcadia build or run on x86_64 from maven
        String libname = "tvmauth_java";
        if (tryLoadResource(libname)) {
            return;
        }

        throw new FileNotFoundException("File " + libname + " was not found inside JAR.");
    }

    private static boolean tryLoadResource(String libname) throws IOException {
        try (InputStream is = getResourceStream(libname)) {
            if (is == null) {
                return false;
            }

            String tempDir = System.getProperty("java.io.tmpdir");
            Path tempFile = Files.createTempFile(Paths.get(tempDir), "tvmauth_java_", ".tmp");

            try {
                Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
                System.load(tempFile.toString());
            } finally {
                if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                    File f = new File(tempFile.toString());
                    f.deleteOnExit();
                } else {
                    Files.delete(tempFile);
                }
            }
        }

        return true;
    }

    private static InputStream getResourceStream(String name) {
        return JniUtils.class.getResourceAsStream("/" + System.mapLibraryName(name));
    }
}
