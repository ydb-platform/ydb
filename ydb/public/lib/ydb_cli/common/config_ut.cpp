#include "config.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(ConfigurationManagerTests) {

    Y_UNIT_TEST(CreateEmptyConfig) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT(!config.Root().IsDefined() || config.Root().IsNull() || config.Root().IsMap());
        }

        // File should not be created for empty config
        UNIT_ASSERT(!configPath.Exists());
    }

    Y_UNIT_TEST(SetAndGetString) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["key"].SetString("value");
        }

        // Reload and verify
        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["key"].AsString(), "value");
        }
    }

    Y_UNIT_TEST(SetAndGetBool) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["enabled"].SetBool(true);
            config["disabled"].SetBool(false);
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["enabled"].AsBool(), true);
            UNIT_ASSERT_VALUES_EQUAL(config["disabled"].AsBool(), false);
        }
    }

    Y_UNIT_TEST(SetAndGetInt) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["port"].SetInt(8080);
            config["negative"].SetInt(-42);
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["port"].AsInt(), 8080);
            UNIT_ASSERT_VALUES_EQUAL(config["negative"].AsInt(), -42);
        }
    }

    Y_UNIT_TEST(NestedKeys) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["section.key"].SetString("nested_value");
            config["section.subsection.deep"].SetInt(123);
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["section.key"].AsString(), "nested_value");
            UNIT_ASSERT_VALUES_EQUAL(config["section.subsection.deep"].AsInt(), 123);

            // Access via chained operators
            UNIT_ASSERT_VALUES_EQUAL(config["section"]["key"].AsString(), "nested_value");
            UNIT_ASSERT_VALUES_EQUAL(config["section"]["subsection"]["deep"].AsInt(), 123);
        }
    }

    Y_UNIT_TEST(DefaultValues) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        TConfigurationManager config(configPath);

        UNIT_ASSERT_VALUES_EQUAL(config["missing"].AsString("default"), "default");
        UNIT_ASSERT_VALUES_EQUAL(config["missing"].AsBool(true), true);
        UNIT_ASSERT_VALUES_EQUAL(config["missing"].AsInt(42), 42);
        UNIT_ASSERT_VALUES_EQUAL(config["missing"].AsDouble(3.14), 3.14);
    }

    Y_UNIT_TEST(OverwriteValue) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["key"].SetString("first");
        }

        {
            TConfigurationManager config(configPath);
            config["key"].SetString("second");
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["key"].AsString(), "second");
        }
    }

    Y_UNIT_TEST(RemoveKey) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["keep"].SetString("kept");
            config["remove"].SetString("removed");
        }

        {
            TConfigurationManager config(configPath);
            config["remove"].Remove();
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["keep"].AsString(), "kept");
            UNIT_ASSERT(!config["remove"].IsDefined() || config["remove"].IsNull());
        }
    }

    Y_UNIT_TEST(RemoveNestedKey) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["section.key1"].SetString("value1");
            config["section.key2"].SetString("value2");
        }

        {
            TConfigurationManager config(configPath);
            config["section.key1"].Remove();
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT(!config["section.key1"].IsDefined() || config["section.key1"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL(config["section.key2"].AsString(), "value2");
        }
    }

    Y_UNIT_TEST(PreserveOtherKeys) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["a"].SetString("A");
            config["b"].SetString("B");
        }

        {
            TConfigurationManager config(configPath);
            config["c"].SetString("C");
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["a"].AsString(), "A");
            UNIT_ASSERT_VALUES_EQUAL(config["b"].AsString(), "B");
            UNIT_ASSERT_VALUES_EQUAL(config["c"].AsString(), "C");
        }
    }

    Y_UNIT_TEST(NodeTypeChecks) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["scalar"].SetString("value");
            config["nested.key"].SetString("nested");
        }

        {
            TConfigurationManager config(configPath);

            auto scalar = config["scalar"];
            UNIT_ASSERT(scalar.IsDefined());
            UNIT_ASSERT(scalar.IsScalar());
            UNIT_ASSERT(!scalar.IsMap());
            UNIT_ASSERT(!scalar.IsNull());

            auto nested = config["nested"];
            UNIT_ASSERT(nested.IsDefined());
            UNIT_ASSERT(nested.IsMap());
            UNIT_ASSERT(!nested.IsScalar());

            auto missing = config["missing"];
            UNIT_ASSERT(!missing.IsDefined() || missing.IsNull());
        }
    }

    Y_UNIT_TEST(ReloadConfig) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        TConfigurationManager config(configPath);
        config["key"].SetString("original");

        // Manually overwrite file
        {
            TFileOutput file(configPath);
            file << "key: modified\n";
        }

        // Before reload - still sees old value (cached)
        UNIT_ASSERT_VALUES_EQUAL(config["key"].AsString(), "original");

        // After reload - sees new value
        config.Reload();
        UNIT_ASSERT_VALUES_EQUAL(config["key"].AsString(), "modified");
    }

    Y_UNIT_TEST(DeeplyNestedPath) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["a.b.c.d.e"].SetString("deep");
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["a.b.c.d.e"].AsString(), "deep");
            UNIT_ASSERT_VALUES_EQUAL(config["a"]["b"]["c"]["d"]["e"].AsString(), "deep");
        }
    }

    Y_UNIT_TEST(MixedTypes) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        {
            TConfigurationManager config(configPath);
            config["string"].SetString("text");
            config["bool"].SetBool(true);
            config["int"].SetInt(42);
        }

        {
            TConfigurationManager config(configPath);
            UNIT_ASSERT_VALUES_EQUAL(config["string"].AsString(), "text");
            UNIT_ASSERT_VALUES_EQUAL(config["bool"].AsBool(), true);
            UNIT_ASSERT_VALUES_EQUAL(config["int"].AsInt(), 42);
        }
    }

    Y_UNIT_TEST(GetPath) {
        TTempDir tempDir;
        TFsPath configPath = tempDir.Path() / "config.yaml";

        TConfigurationManager config(configPath);
        UNIT_ASSERT_VALUES_EQUAL(config.GetPath(), configPath);

        auto node = config["section.key"];
        UNIT_ASSERT_VALUES_EQUAL(node.GetPath(), "section.key");
    }

}
