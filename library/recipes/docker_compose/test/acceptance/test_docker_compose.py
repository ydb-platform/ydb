# coding: utf-8

import os

import test.tests.common as tests_common
import yatest.common


class TestDockerCompose(tests_common.YaTest):

    def test_run(self):
        res = self.run_ya_make_test(cwd=yatest.common.source_path("library/recipes/docker_compose/example"), args=["-A", "--test-type", "py3test"])
        assert res.get_tests_count() == 1
        assert res.get_suites_count() == 1

        res.verify_test("test.py", "test_compose_works", "OK")
        for output_type in ['std_out', 'std_err']:
            assert os.path.exists(os.path.join(
                res.output_root, "library/recipes/docker_compose/example",
                "test-results", "py3test", "testing_out_stuff", "containers", "example_redis_1", "container_{}.log".format(output_type)
            ))
            assert os.path.exists(os.path.join(
                res.output_root, "library/recipes/docker_compose/example",
                "test-results", "py3test", "testing_out_stuff", "containers", "example_web_1", "container_{}.log".format(output_type)
            ))

    def test_run_with_context(self):
        res = self.run_ya_make_test(cwd=yatest.common.source_path("library/recipes/docker_compose/example_with_context"), args=["-A", "--test-type", "py3test"])
        assert res.get_tests_count() == 1
        assert res.get_suites_count() == 1

        res.verify_test("test.py", "test_compose_works", "OK")

    def test_run_test_in_container(self):
        with open("stdin", "wb") as stdin:
            # need to pass stdin as docker-compose exec needs it (it runs `docker exec --interactive`)
            res = self.run_ya_make_test(
                cwd=yatest.common.source_path("library/recipes/docker_compose/example_test_container"), args=["-A", "--test-type", "py3test"], stdin=stdin)
        assert res.get_tests_count() == 1
        assert res.get_suites_count() == 1

        res.verify_test("test.py", "test_compose_works", "OK")

    def test_invalid_test_container_name(self):
        res = self.run_ya_make_test(cwd=yatest.common.test_source_path("data/invalid_test_container_name"), args=["-A", "--test-type", "py3test"])
        assert res.get_tests_count() == 0
        assert res.get_suites_count() == 1
        assert "Service with name 'not_existing_container_name' was not found to be setup as a host for running test" in res.err

    def test_container_with_existing_command(self):
        res = self.run_ya_make_test(cwd=yatest.common.test_source_path("data/test_container_with_existing_command"), args=["-A", "--test-type", "py3test"])
        assert res.get_tests_count() == 0
        assert res.get_suites_count() == 1
        assert "Test hosting service 'test' has `command` section which is not supported by testing framework" in res.err

    def test_container_with_existing_user(self):
        res = self.run_ya_make_test(cwd=yatest.common.test_source_path("data/test_container_with_existing_user"), args=["-A", "--test-type", "py3test"])
        assert res.get_tests_count() == 0
        assert res.get_suites_count() == 1
        assert "Test hosting service 'test' has `user` section which is not supported by testing framework" in res.err

    def test_run_with_recipe_config(self):
        with open("stdin", "wb") as stdin:
            # need to pass stdin as docker-compose exec needs it (it runs `docker exec --interactive`
            res = self.run_ya_make_test(
                cwd=yatest.common.source_path("library/recipes/docker_compose/example_with_recipe_config"),
                args=["-A", "--test-type", "py3test"],
                stdin=stdin
            )

        assert res.get_tests_count() == 1
        assert res.get_suites_count() == 1

        res.verify_test("test.py", "test", "OK")

        assert os.path.exists(os.path.join(
            res.output_root,
            "library/recipes/docker_compose/example_with_recipe_config/test-results/py3test/testing_out_stuff/containers/py3test_test_1/output/",
            "out.txt",
        ))

    def test_recipe_container_exit_0(self):
        res = self.run_ya_make_test(cwd=yatest.common.test_source_path("data/test_recipe_container_exit_0"),
                                    args=["-A", "--test-type", "py3test"])
        res.verify_test("test.py", "test_simple", "OK")

    def test_recipe_container_fail(self):
        res = self.run_ya_make_test(cwd=yatest.common.test_source_path("data/test_recipe_container_fail"),
                                    args=["-A", "--test-type", "py3test"])
        assert "DockerComposeRecipeException" in res.err
        assert "Has failed containers" in res.err
        assert "srv1" in res.err
