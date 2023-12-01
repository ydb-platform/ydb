import logging
import os
import shutil
from library.python.testing.recipe import declare_recipe
import yatest.common


class ChameleonRecipe:
    def __init__(self):
        self.bin2_root: str = None
        self.bin3_root: str = None
        self.bin2: str = None
        self.bin3: str = None

    def post_init(self):
        self.bin2_root = yatest.common.build_path("devtools/ya/bin")
        self.bin3_root = yatest.common.build_path("devtools/ya/bin3")
        self.bin2 = os.path.join(self.bin2_root, "ya-bin")
        self.bin3 = os.path.join(self.bin3_root, "ya-bin")

        logging.info("ya-bin2 exists: %s", os.path.exists(self.bin2))
        logging.info("ya-bin3 exists: %s", os.path.exists(self.bin3))

    @classmethod
    def check_argv_py3(cls, argv):
        return argv[0] == "yes"

    def start(self, argv):
        self.post_init()

        logging.info("args: %s", argv)

        assert not (os.path.exists(self.bin2) and os.path.exists(self.bin3)), "Only one version of ya-bin can be in DEPENDS"

        if self.check_argv_py3(argv):
            if not os.path.exists(self.bin2):
                if not os.path.exists(self.bin2_root):
                    os.mkdir(self.bin2_root)
                    logging.debug("Create bin2 root folder: %s", self.bin2_root)
                else:
                    logging.debug("Maybe test was restarted")
                shutil.copy(self.bin3, self.bin2)
                logging.debug("copy ya-bin3 to bin2 root: %s -> %s", self.bin3, self.bin2)


    def stop(self, argv):
        self.post_init()

        logging.info("args: %s", argv)

        if self.check_argv_py3(argv):
            if os.path.exists(self.bin2):
                logging.debug("Remove bin2 executable: %s", self.bin2)
                # TOFIX v-korovin
                os.remove(self.bin2)

if __name__ == "__main__":
    recipe = ChameleonRecipe()
    declare_recipe(recipe.start, recipe.stop)
