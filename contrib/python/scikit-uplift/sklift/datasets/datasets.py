import hashlib
import os
import shutil

import pandas as pd
import requests
from sklearn.utils import Bunch
from tqdm.auto import tqdm


def get_data_dir():
    """Return the path of the scikit-uplift data dir.

    This folder is used by some large dataset loaders to avoid downloading the data several times.

    By default the data dir is set to a folder named ``scikit-uplift-data`` in the user home folder.

    Returns:
        string: The path to scikit-uplift data dir.
    """
    return os.path.join(os.path.expanduser("~"), "scikit-uplift-data")


def _create_data_dir(path):
    """Creates a directory, which stores the datasets.

    Args:
        path (str): The path to scikit-uplift data dir.
    """
    if not os.path.isdir(path):
        os.makedirs(path)


def _download(url, dest_path, content_length_header_key='Content-Length', desc=None):
    """Download the file from url and save it locally.

    Args:
        url (str): URL address, must be a string.
        dest_path (str): Destination of the file.
        content_length_header_key (str): The key in the HTTP response headers that lists the response size in bytes.
            Used for progress bar.
    """
    if isinstance(url, str):
        req = requests.get(url, stream=True)
        req.raise_for_status()

        with open(dest_path, "wb") as fd:
            total_size_in_bytes = int(req.headers.get(content_length_header_key, 0))
            progress_bar = tqdm(desc=desc, total=total_size_in_bytes, unit='iB', unit_scale=True)
            for chunk in req.iter_content(chunk_size=2 ** 20):
                progress_bar.update(len(chunk))
                fd.write(chunk)
    else:
        raise TypeError("URL must be a string")


def _get_data(data_home, url, dest_subdir, dest_filename, download_if_missing,
              content_length_header_key='Content-Length', desc=None):
    """Return the path to the dataset.

    Args:
        data_home (str): The path to scikit-uplift data dir.
        url (str): The URL to the dataset.
        dest_subdir (str): The name of the folder in which the dataset is stored.
        dest_filename (str): The name of the dataset.
        download_if_missing (bool): If False, raise a IOError if the data is not locally available instead of
            trying to download the data from the source site.
        content_length_header_key (str): The key in the HTTP response headers that lists the response size in bytes.
            Used for progress bar.

    Returns:
        string: The path to the dataset.
    """
    if data_home is None:
        if dest_subdir is None:
            data_dir = get_data_dir()
        else:
            data_dir = os.path.join(get_data_dir(), dest_subdir)
    else:
        if dest_subdir is None:
            data_dir = os.path.abspath(data_home)
        else:
            data_dir = os.path.join(os.path.abspath(data_home), dest_subdir)

    _create_data_dir(data_dir)

    dest_path = os.path.join(data_dir, dest_filename)

    if not os.path.isfile(dest_path):
        if download_if_missing:
            _download(url, dest_path, content_length_header_key, desc)
        else:
            raise IOError("Dataset missing")
    return dest_path


def _get_file_hash(path):
    """Сompute the hash value for a file by using md5 algorithm.

        Args:
            path (str): The path to file
    """
    with open(path, 'rb') as file_to_check:
        data = file_to_check.read()    
        return hashlib.md5(data).hexdigest()


def clear_data_dir(path=None):
    """Delete all the content of the data home cache.

        Args:
            path (str): The path to scikit-uplift data dir
    """
    if path is None:
        path = get_data_dir()
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)


def fetch_lenta(data_home=None, dest_subdir=None, download_if_missing=True, return_X_y_t=False):
    """Load and return the Lenta dataset (classification).

    An uplift modeling dataset containing data about Lenta's customers grociery shopping and
    related marketing campaigns.

    Major columns:

    - ``group`` (str): treatment/control group flag
    - ``response_att`` (binary): target
    - ``gender`` (str): customer gender
    - ``age`` (float): customer age
    - ``main_format`` (int): store type (1 - grociery store, 0 - superstore)

    Read more in the :ref:`docs <Lenta>`.

    Args:
        data_home (str): The path to the folder where datasets are stored.
        dest_subdir (str): The name of the folder in which the dataset is stored.
        download_if_missing (bool): Download the data if not present. Raises an IOError if False and data is missing.
        return_X_y_t (bool): If True, returns (data, target, treatment) instead of a Bunch object.

    Returns:
        Bunch or tuple: dataset.

        Bunch:
            By default dictionary-like object, with the following attributes:

                * ``data`` (DataFrame object): Dataset without target and treatment.
                * ``target`` (Series object): Column target by values.
                * ``treatment`` (Series object): Column treatment by values.
                * ``DESCR`` (str): Description of the Lenta dataset.
                * ``feature_names`` (list): Names of the features.
                * ``target_name`` (str): Name of the target.
                * ``treatment_name`` (str): Name of the treatment.

        Tuple:
            tuple (data, target, treatment) if `return_X_y_t` is True

    Example::

        from sklift.datasets import fetch_lenta


        dataset = fetch_lenta()
        data, target, treatment = dataset.data, dataset.target, dataset.treatment

        # alternative option
        data, target, treatment = fetch_lenta(return_X_y_t=True)

    See Also:

        :func:`.fetch_x5`: Load and return the X5 RetailHero dataset (classification).

        :func:`.fetch_criteo`: Load and return the Criteo Uplift Prediction Dataset (classification).

        :func:`.fetch_hillstrom`: Load and return Kevin Hillstrom Dataset MineThatData (classification or regression).

        :func:`.fetch_megafon`: Load and return the MegaFon Uplift Competition dataset (classification).
    """
    lenta_metadata = {
        'desc': 'Lenta dataset',
        'url': 'https://sklift.s3.eu-west-2.amazonaws.com/lenta_dataset.csv.gz',
        'hash': '6ab28ff0989ed8b8647f530e2e86452f'
    }

    filename = lenta_metadata['url'].split('/')[-1]
    csv_path = _get_data(data_home=data_home, url=lenta_metadata['url'], dest_subdir=dest_subdir,
                         dest_filename=filename,
                         download_if_missing=download_if_missing,
                         desc=lenta_metadata['desc'])
    
    if _get_file_hash(csv_path) != lenta_metadata['hash']:
        raise ValueError(f"The {filename} file is broken, please clean the directory "
                         f"with the clean_data_dir() function, and run the function again")

    target_col = 'response_att'
    treatment_col = 'group'

    data = pd.read_csv(csv_path)
    treatment, target = data[treatment_col], data[target_col]

    data = data.drop([target_col, treatment_col], axis=1)
    feature_names = list(data.columns)

    if return_X_y_t:
        return data, target, treatment

    module_path = os.path.dirname(__file__)
    with open(os.path.join(module_path, 'descr', 'lenta.rst')) as rst_file:
        fdescr = rst_file.read()

    return Bunch(data=data, target=target, treatment=treatment, DESCR=fdescr,
                 feature_names=feature_names, target_name=target_col, treatment_name=treatment_col)


def fetch_x5(data_home=None, dest_subdir=None, download_if_missing=True):
    """Load and return the X5 RetailHero dataset (classification).

    The dataset contains raw retail customer purchases, raw information about products and general info about customers.

    Major columns:

    - ``treatment_flg`` (binary): treatment/control group flag
    - ``target`` (binary): target
    - ``customer_id`` (str): customer id - primary key for joining

    Read more in the :ref:`docs <X5>`.

    Args:
        data_home (str, unicode): The path to the folder where datasets are stored.
        dest_subdir (str, unicode): The name of the folder in which the dataset is stored.
        download_if_missing (bool): Download the data if not present. Raises an IOError if False and data is missing

    Returns:
        Bunch: dataset.

         Dictionary-like object, with the following attributes.

            * ``data`` (Bunch object): dictionary-like object without target and treatment:

                * ``clients`` (ndarray or DataFrame object): General info about clients.
                * ``train`` (ndarray or DataFrame object): A subset of clients for training.
                * ``purchases`` (ndarray or DataFrame object): clients’ purchase history prior to communication.
            * ``target`` (Series object): Column target by values.
            * ``treatment`` (Series object): Column treatment by values.
            * ``DESCR`` (str): Description of the X5 dataset.
            * ``feature_names`` (Bunch object): Names of the features.
            * ``target_name`` (str): Name of the target.
            * ``treatment_name`` (str): Name of the treatment.

    References:
        https://ods.ai/competitions/x5-retailhero-uplift-modeling/data

    Example::

        from sklift.datasets import fetch_x5


        dataset = fetch_x5()
        data, target, treatment = dataset.data, dataset.target, dataset.treatment

        # data - dictionary-like object
        # data contains general info about clients:
        clients = data.clients

        # data contains a subset of clients for training:
        train = data.train

        # data contains a clients’ purchase history prior to communication.
        purchases = data.purchases

    See Also:

        :func:`.fetch_lenta`: Load and return the Lenta dataset (classification).

        :func:`.fetch_criteo`: Load and return the Criteo Uplift Prediction Dataset (classification).

        :func:`.fetch_hillstrom`: Load and return Kevin Hillstrom Dataset MineThatData (classification or regression).

        :func:`.fetch_megafon`: Load and return the MegaFon Uplift Competition dataset (classification).
    """
    x5_metadata = {
        'desc_train': 'Part 1: X5 train',
        'desc_clients': 'Part 2: X5 clients',
        'desc_purchases': 'Part 3: X5 purchases',
        'url_train': 'https://sklift.s3.eu-west-2.amazonaws.com/uplift_train.csv.gz',
        'url_clients': 'https://sklift.s3.eu-west-2.amazonaws.com/clients.csv.gz',
        'url_purchases': 'https://sklift.s3.eu-west-2.amazonaws.com/purchases.csv.gz',
        'hash_train': '2720bbb659daa9e0989b2777b6a42d19',
        'hash_clients': 'b9cdeb2806b732771de03e819b3354c5',
        'hash_purchases': '48d2de13428e24e8b61d66fef02957a8'
    }
    file_train = x5_metadata['url_train'].split('/')[-1]
    csv_train_path = _get_data(data_home=data_home, url=x5_metadata['url_train'], dest_subdir=dest_subdir,
                               dest_filename=file_train,
                               download_if_missing=download_if_missing,
                               desc=x5_metadata['desc_train'])

    if _get_file_hash(csv_train_path) != x5_metadata['hash_train']:
        raise ValueError(f"The {file_train} file is broken, please clean the directory "
                         f"with the clean_data_dir() function, and run the function again")

    train = pd.read_csv(csv_train_path)
    train_features = list(train.columns)

    target_col = 'target'
    treatment_col = 'treatment_flg'

    treatment, target = train[treatment_col], train[target_col]

    train = train.drop([target_col, treatment_col], axis=1)

    file_clients = x5_metadata['url_clients'].split('/')[-1]
    csv_clients_path = _get_data(data_home=data_home, url=x5_metadata['url_clients'], dest_subdir=dest_subdir,
                                 dest_filename=file_clients,
                                 download_if_missing=download_if_missing,
                                 desc=x5_metadata['desc_clients'])

    if _get_file_hash(csv_clients_path) != x5_metadata['hash_clients']:
        raise ValueError(f"The {file_clients} file is broken, please clean the directory "
                         f"with the clean_data_dir() function, and run the function again")

    clients = pd.read_csv(csv_clients_path)
    clients_features = list(clients.columns)

    file_purchases = x5_metadata['url_purchases'].split('/')[-1]
    csv_purchases_path = _get_data(data_home=data_home, url=x5_metadata['url_purchases'], dest_subdir=dest_subdir,
                                   dest_filename=file_purchases,
                                   download_if_missing=download_if_missing,
                                   desc=x5_metadata['desc_purchases'])

    if _get_file_hash(csv_purchases_path) != x5_metadata['hash_purchases']:
        raise ValueError(f"The {file_purchases} file is broken, please clean the directory "
                         f"with the clean_data_dir() function, and run the function again")
             
    purchases = pd.read_csv(csv_purchases_path)
    purchases_features = list(purchases.columns)

    data = Bunch(clients=clients, train=train, purchases=purchases)
    feature_names = Bunch(train_features=train_features, clients_features=clients_features,
                          purchases_features=purchases_features)

    module_path = os.path.dirname(__file__)
    with open(os.path.join(module_path, 'descr', 'x5.rst')) as rst_file:
        fdescr = rst_file.read()

    return Bunch(data=data, target=target, treatment=treatment, DESCR=fdescr,
                 feature_names=feature_names, target_name='target', treatment_name='treatment_flg')


def fetch_criteo(target_col='visit', treatment_col='treatment', data_home=None, dest_subdir=None,
                 download_if_missing=True, percent10=False, return_X_y_t=False):
    """Load and return the Criteo Uplift Prediction Dataset (classification).

    This dataset is constructed by assembling data resulting from several incrementality tests, a particular randomized
    trial procedure where a random part of the population is prevented from being targeted by advertising.

    Major columns:

    * ``treatment`` (binary): treatment
    * ``exposure`` (binary): treatment
    * ``visit`` (binary): target
    * ``conversion`` (binary): target
    * ``f0, ... , f11`` (float): feature values

    Read more in the :ref:`docs <Criteo>`.

    Args:
        target_col (string, 'visit', 'conversion' or 'all', default='visit'): Selects which column from dataset
            will be target. If 'all', return a DataFrame with all targets cols.
        treatment_col (string,'treatment', 'exposure' or 'all', default='treatment'): Selects which column from dataset
            will be treatment. If 'all', return a DataFrame with all treatment cols.
        data_home (string): Specify a download and cache folder for the datasets.
        dest_subdir (string): The name of the folder in which the dataset is stored.
        download_if_missing (bool, default=True): If False, raise an IOError if the data is not locally available
            instead of trying to download the data from the source site.
        percent10 (bool, default=False): Whether to load only 10 percent of the data.
        return_X_y_t (bool, default=False): If True, returns (data, target, treatment) instead of a Bunch object.

    Returns:
        Bunch or tuple: dataset.

        Bunch:
            By default dictionary-like object, with the following attributes:

                * ``data`` (DataFrame object): Dataset without target and treatment.
                * ``target`` (Series or DataFrame object): Column target by values.
                * ``treatment`` (Series or DataFrame object): Column treatment by values.
                * ``DESCR`` (str): Description of the Criteo dataset.
                * ``feature_names`` (list): Names of the features.
                * ``target_name`` (str list): Name of the target.
                * ``treatment_name`` (str or list): Name of the treatment.

        Tuple:
            tuple (data, target, treatment) if `return_X_y` is True

    Example::

        from sklift.datasets import fetch_criteo


        dataset = fetch_criteo(target_col='conversion', treatment_col='exposure')
        data, target, treatment = dataset.data, dataset.target, dataset.treatment

        # alternative option
        data, target, treatment = fetch_criteo(target_col='conversion', treatment_col='exposure', return_X_y_t=True)

    References:
        :cite:t:`Diemert2018`

        .. bibliography::

    See Also:

        :func:`.fetch_lenta`: Load and return the Lenta dataset (classification).

        :func:`.fetch_x5`: Load and return the X5 RetailHero dataset (classification).

        :func:`.fetch_hillstrom`: Load and return Kevin Hillstrom Dataset MineThatData (classification or regression).

        :func:`.fetch_megafon`: Load and return the MegaFon Uplift Competition dataset (classification).
    """
    treatment_cols = ['exposure', 'treatment']
    if treatment_col == 'all':
        treatment_col = treatment_cols
    elif treatment_col not in treatment_cols:
        raise ValueError(f"The treatment_col must be an element of {treatment_cols + ['all']}. "
                         f"Got value target_col={treatment_col}.")

    target_cols = ['visit', 'conversion']
    if target_col == 'all':
        target_col = target_cols
    elif target_col not in target_cols:
        raise ValueError(f"The target_col must be an element of {target_cols + ['all']}. "
                         f"Got value target_col={target_col}.")

    if percent10:
        criteo_metadata = {
            'desc': 'Criteo dataset (10 percent)',
            'url': 'https://criteo-bucket.s3.eu-central-1.amazonaws.com/criteo10.csv.gz',
            'hash': 'fe159bcee2cea57548e48eb2d7d5d00c'
        }
    else:
        criteo_metadata = {
            'desc': 'Criteo dataset',
            'url': 'https://criteo-bucket.s3.eu-central-1.amazonaws.com/criteo.csv.gz',
            'hash': 'd2236769ef69e9be52556110102911ec'
        }

    filename = criteo_metadata['url'].split('/')[-1]
    csv_path = _get_data(data_home=data_home, url=criteo_metadata['url'], dest_subdir=dest_subdir,
                         dest_filename=filename,
                         download_if_missing=download_if_missing,
                         desc=criteo_metadata['desc'])

    if _get_file_hash(csv_path) != criteo_metadata['hash']:
        raise ValueError(f"The {filename} file is broken, please clean the directory "
                         f"with the clean_data_dir() function, and run the function again")

    dtypes = {
        'exposure': 'Int8',
        'treatment': 'Int8',
        'conversion': 'Int8',
        'visit': 'Int8'
    }
    data = pd.read_csv(csv_path, dtype=dtypes)
    treatment, target = data[treatment_col], data[target_col]

    data = data.drop(target_cols + treatment_cols, axis=1)

    if return_X_y_t:
        return data, target, treatment

    feature_names = list(data.columns)

    module_path = os.path.dirname(__file__)
    with open(os.path.join(module_path, 'descr', 'criteo.rst')) as rst_file:
        fdescr = rst_file.read()

    return Bunch(data=data, target=target, treatment=treatment, DESCR=fdescr, feature_names=feature_names,
                 target_name=target_col, treatment_name=treatment_col)


def fetch_hillstrom(target_col='visit', data_home=None, dest_subdir=None, download_if_missing=True,
                    return_X_y_t=False):
    """Load and return Kevin Hillstrom Dataset MineThatData (classification or regression).

    This dataset contains 64,000 customers who last purchased within twelve months.
    The customers were involved in an e-mail test.

    Major columns:

    * ``visit`` (binary): target. 1/0 indicator, 1 = Customer visited website in the following two weeks.
    * ``conversion`` (binary): target. 1/0 indicator, 1 = Customer purchased merchandise in the following two weeks.
    * ``spend`` (float): target. Actual dollars spent in the following two weeks.
    * ``segment`` (str): treatment. The e-mail campaign the customer received

    Read more in the :ref:`docs <Hillstrom>`.

    Args:
        target_col (string, 'visit' or 'conversion', 'spend' or 'all', default='visit'): Selects which column from dataset
            will be target
        data_home (str): The path to the folder where datasets are stored.
        dest_subdir (str): The name of the folder in which the dataset is stored.
        download_if_missing (bool): Download the data if not present. Raises an IOError if False and data is missing.
        return_X_y_t (bool, default=False): If True, returns (data, target, treatment) instead of a Bunch object.

    Returns:
        Bunch or tuple: dataset.

        Bunch:
            By default dictionary-like object, with the following attributes:

                * ``data`` (DataFrame object): Dataset without target and treatment.
                * ``target`` (Series or DataFrame object): Column target by values.
                * ``treatment`` (Series object): Column treatment by values.
                * ``DESCR`` (str): Description of the Hillstrom dataset.
                * ``feature_names`` (list): Names of the features.
                * ``target_name`` (str or list): Name of the target.
                * ``treatment_name`` (str): Name of the treatment.

        Tuple:
            tuple (data, target, treatment) if `return_X_y` is True

    References:
        https://blog.minethatdata.com/2008/03/minethatdata-e-mail-analytics-and-data.html

    Example::

        from sklift.datasets import fetch_hillstrom


        dataset = fetch_hillstrom(target_col='visit')
        data, target, treatment = dataset.data, dataset.target, dataset.treatment

        # alternative option
        data, target, treatment = fetch_hillstrom(target_col='visit', return_X_y_t=True)

    See Also:

        :func:`.fetch_lenta`: Load and return the Lenta dataset (classification).

        :func:`.fetch_x5`: Load and return the X5 RetailHero dataset (classification).

        :func:`.fetch_criteo`: Load and return the Criteo Uplift Prediction Dataset (classification).

        :func:`.fetch_megafon`: Load and return the MegaFon Uplift Competition dataset (classification)
    """
    target_cols = ['visit', 'conversion', 'spend']
    if target_col == 'all':
        target_col = target_cols
    elif target_col not in target_cols:
        raise ValueError(f"The target_col must be an element of {target_cols + ['all']}. "
                         f"Got value target_col={target_col}.")

    hillstrom_metadata = {
        'desc': 'Hillstrom dataset',
        'url': 'https://hillstorm1.s3.us-east-2.amazonaws.com/hillstorm_no_indices.csv.gz',
        'hash': 'a68a81291f53a14f4e29002629803ba3'
    }
 
    filename = hillstrom_metadata['url'].split('/')[-1]
    csv_path = _get_data(data_home=data_home, url=hillstrom_metadata['url'], dest_subdir=dest_subdir,
                         dest_filename=filename,
                         download_if_missing=download_if_missing,
                         desc=hillstrom_metadata['desc'])
    
    if _get_file_hash(csv_path) != hillstrom_metadata['hash']:
        raise ValueError(f"The {filename} file is broken, please clean the directory "
                         f"with the clean_data_dir() function, and run the function again")

    treatment_col = 'segment'

    data = pd.read_csv(csv_path)
    treatment, target = data[treatment_col], data[target_col]

    data = data.drop(target_cols + [treatment_col], axis=1)

    if return_X_y_t:
        return data, target, treatment

    feature_names = list(data.columns)

    module_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(module_path, 'descr', 'hillstrom.rst')) as rst_file:
        fdescr = rst_file.read()

    return Bunch(data=data, target=target, treatment=treatment, DESCR=fdescr,
                 feature_names=feature_names, target_name=target_col, treatment_name=treatment_col)


def fetch_megafon(data_home=None, dest_subdir=None, download_if_missing=True,
                  return_X_y_t=False):
    """Load and return the MegaFon Uplift Competition dataset (classification).

    An uplift modeling dataset containing synthetic data generated by telecom companies, trying to bring them closer to the real case that they encountered.

    Major columns:

    - ``X_1...X_50`` : anonymized feature set
    - ``conversion`` (binary): target
    - ``treatment_group`` (str): customer purchasing

    Read more in the :ref:`docs <MegaFon>`.

    Args:
        data_home (str): The path to the folder where datasets are stored.
        dest_subdir (str): The name of the folder in which the dataset is stored.
        download_if_missing (bool): Download the data if not present. Raises an IOError if False and data is missing.
        return_X_y_t (bool): If True, returns (data, target, treatment) instead of a Bunch object.

    Returns:
        Bunch or tuple: dataset.

        Bunch:
            By default dictionary-like object, with the following attributes:

                * ``data`` (DataFrame object): Dataset without target and treatment.
                * ``target`` (Series object): Column target by values.
                * ``treatment`` (Series object): Column treatment by values.
                * ``DESCR`` (str): Description of the Megafon dataset.
                * ``feature_names`` (list): Names of the features.
                * ``target_name`` (str): Name of the target.
                * ``treatment_name`` (str): Name of the treatment.

        Tuple:
            tuple (data, target, treatment) if `return_X_y` is True

    Example::

        from sklift.datasets import fetch_megafon


        dataset = fetch_megafon()
        data, target, treatment = dataset.data, dataset.target, dataset.treatment

        # alternative option
        data, target, treatment = fetch_megafon(return_X_y_t=True)

    See Also:

        :func:`.fetch_lenta`: Load and return the Lenta dataset (classification).

        :func:`.fetch_x5`: Load and return the X5 RetailHero dataset (classification).

        :func:`.fetch_criteo`: Load and return the Criteo Uplift Prediction Dataset (classification).

        :func:`.fetch_hillstrom`: Load and return Kevin Hillstrom Dataset MineThatData (classification or regression).
    """
    megafon_metadata = {
        'desc': 'Megafon dataset',
        'url': 'https://sklift.s3.eu-west-2.amazonaws.com/megafon_dataset.csv.gz',
        'hash': 'ee8d45a343d4d2cf90bb756c93959ecd'
    }

    filename = megafon_metadata['url'].split('/')[-1]
    csv_path = _get_data(data_home=data_home, url=megafon_metadata['url'], dest_subdir=dest_subdir,
                         dest_filename=filename,
                         download_if_missing=download_if_missing,
                         desc=megafon_metadata['desc'])
    
    if _get_file_hash(csv_path) != megafon_metadata['hash']:
        raise ValueError(f"The {filename} file is broken, please clean the directory "
                         f"with the clean_data_dir() function, and run the function again")
        
    train = pd.read_csv(csv_path)

    target_col = 'conversion'
    treatment_col = 'treatment_group'

    treatment, target = train[treatment_col], train[target_col]

    train = train.drop([target_col, treatment_col], axis=1)

    if return_X_y_t:
        return train, target, treatment

    feature_names = list(train.columns)

    module_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(module_path, 'descr', 'megafon.rst')) as rst_file:
        fdescr = rst_file.read()

    return Bunch(data=train, target=target, treatment=treatment, DESCR=fdescr,
                 feature_names=feature_names, target_name=target_col, treatment_name=treatment_col)
