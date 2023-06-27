#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys


def main():
    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    tlds = dict()

    for s in alphabet:
        tlds[s] = list()
    tlds['xn--'] = list()

    tld_file = open(sys.argv[1], 'rb')
    for line in tld_file.readlines():
        domain = line.strip().lower()
        for label in tlds:
            if domain.startswith(b'xn--'):
                tlds['xn--'].append(domain)
                break
            elif domain.startswith(b'x'):
                tlds['x'].append(domain)
                break
            else:
                if domain.startswith(label.encode('utf-8')):
                    tlds[label].append(domain)
                    break

    stdout = open(sys.stdout.fileno(), "w", encoding="utf-8", closefd=False)

    stdout.write('// actual list can be found at http://data.iana.org/TLD/tlds-alpha-by-domain.txt\n')
    stdout.write('static const char* const TopLevelDomains[] = {\n')

    for label, value in sorted(tlds.items()):
        if label == 'xn--':
            stdout.write('    /* ')
            str = ''
            for n in value:
                unicode_domain = n.decode('idna')
                str += ('%s, ' % unicode_domain)
            stdout.write('%s*/\n' % str.rstrip())

            stdout.write('    ')
            str = ''
            for n in value:
                str += ('"%s", ' % n.decode('utf-8'))
            stdout.write('%s\n' % str.rstrip())
        else:
            stdout.write('    ')
            str = ''
            for n in value:
                str += ('"%s", ' % n.decode('utf-8'))
            stdout.write('%s\n' % str.rstrip())

    stdout.write('    0\n')
    stdout.write('};\n')

if __name__ == '__main__':
    main()
