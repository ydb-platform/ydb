from requests_html import HTMLSession, MaxRetries
from dataclasses import dataclass
import configparser, os, glob
import requests
from pyppeteer import errors

@dataclass
class UrlStatus:
    status_code: str
    original_url: str
    final_url: str

global config
global session

session = HTMLSession()
config = configparser.ConfigParser()
config.read("settings.ini")

def get_url_status(original_url):
    try:
        url_suffix = '?'+config['DEFAULT']['parameter']+'=' + config['DEFAULT']['target_doc_version']
        full_url = original_url + url_suffix 
        response = session.get(full_url, timeout=120)
        response.html.render()

        # Strip the final url from the version suffix for UrlStatus
        final_url = response.url
        if final_url.endswith(url_suffix):
            final_url = final_url[:-len(url_suffix)]

        status_code = response.status_code

        return UrlStatus(status_code=status_code, original_url=original_url, final_url=final_url)

    #except Exception as e:
    #    print(f"Произошла ошибка: {e}")
    #    return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error: {e}")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error: {e}")
        return None
    except errors.PageError as e:
        print(f"PageError: {e}")
        return None
    except MaxRetries as e:
        print(f"MaxRetries: {e}")
        return None

def main():

    with open("missing-redirects.txt", "w") as f:

        search_pattern = os.path.join(config['DEFAULT']['src_doc'], '**', '*.html')
        for file_path in glob.iglob(search_pattern, recursive=True):
            file_path = file_path[len(config['DEFAULT']['src_doc']):]
            url = config['DEFAULT']['documentation_base_url'] + file_path
            if 'single-page.html' in url: continue
            print(f'''Checking {url}''')
            r = get_url_status(url)
            if r is not None and r.status_code != 200:
                f.write(r.original_url + '\n')
                f.flush()
                os.fsync(f.fileno())
                print(f'''\t{r.status_code}''')
            if r is not None and (r.final_url != r.original_url):
                print(f'''\tREDIRECTED: {r.final_url}''')


if __name__ == "__main__":
    main()
