import os.path
import os
import subprocess
import shlex
import shutil
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


cwd = os.path.dirname(os.path.realpath(__file__))


class Sample():
    def setUp(self):
        super().setUp()
        self.proc = None
        self.outputs = []

    def tearDown(self):
        super().tearDown()
        if self.proc is not None:
            self.proc.stdin.close()
            self.proc.stdout.close()
            self.proc.kill()
    
    def replaceVariables(self, filein ,fileout, vars):
        with open(filein, "rt") as fin:
            with open(fileout, "wt") as fout:
                for line in fin:
                    for k, v in vars.items():
                        line = line.replace(k, v)
                    fout.write(line)

    def run_sample(self, filepath, variables):
        inpath = os.path.join(cwd, "..", "..", "docs", "examples", filepath)
        outpath = os.path.join(cwd, "tmp_{}".format(filepath))
        self.replaceVariables(inpath, outpath, variables)

        self.proc = subprocess.Popen(
            [shutil.which("python"),
             outpath],
            text=True, bufsize=1,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE
        )

    def write(self, string):
        self.proc.stdin.write(string)
        self.proc.stdin.flush()

    def wait_for_pattern(self, pattern):
        try:
            while True:
                line = self.proc.stdout.readline()
                self.outputs.append(line)
                if pattern in line:
                    return line
        except subprocess.TimeoutExpired:
            self.assertTrue(False, "timeout when looking for output")

    def wait_for_end(self):
        try:
            outs, err = self.proc.communicate(timeout=10)
            self.outputs += filter(lambda x: x != '', outs.split('\n'))
        except subprocess.TimeoutExpired:
            self.assertTrue(False, "timeout when looking for output")
        return self.outputs[-1]
            


class Browser():
    def setUp(self):
        super().setUp()
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        self.driver = webdriver.Chrome(options=options)
        self.user_username = os.environ.get("AUTH0_USERNAME")
        self.user_password = os.environ.get("AUTH0_PASSWORD")

        if not self.user_username or not self.user_password:
            self.skipTest("auth0 is not configured properly")

    def tearDown(self):
        super().tearDown()
        self.driver.quit()

    def authorize_auth0(self, authorize_url, expected_redirect_uri):
        self.driver.get(authorize_url)
        username = self.driver.find_element(By.ID, "username")
        password = self.driver.find_element(By.ID, "password")

        wait = WebDriverWait(self.driver, timeout=2)
        wait.until(lambda d : username.is_displayed())
        wait.until(lambda d : password.is_displayed())

        username.clear()
        username.send_keys(self.user_username)
        password.send_keys(self.user_password)
        username.send_keys(Keys.RETURN)

        wait.until(EC.url_contains(expected_redirect_uri))
        return self.driver.current_url

