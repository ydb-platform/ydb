import string
import random

def get_random_id(id_len):
    return ''.join(random.sample(string.ascii_lowercase + string.digits, id_len))

