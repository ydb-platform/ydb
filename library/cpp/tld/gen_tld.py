#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os

def main():
    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    tlds = dict()

    for s in alphabet:
        tlds[s] = list()
    tlds['xn--'] = list()

    tld_file = open(sys.argv[1], 'r')
    for line in tld_file.readlines():
        domain = line.strip().lower()
        for label in tlds:
            if domain.startswith('xn--'):
                tlds['xn--'].append(domain)
                break
            elif domain.startswith('x'):
                tlds['x'].append(domain)
                break
            else:
                if domain.startswith(label):
                    tlds[label].append(domain)
                    break

    print '// actual list can be found at http://data.iana.org/TLD/tlds-alpha-by-domain.txt'
    print 'static const char* const TopLevelDomains[] = {' 

    for label, value in sorted(tlds.iteritems()):
        if label == 'xn--':
            sys.stdout.write('    /* ')
            str = ''
            for n in value:
                unicode_domain = n.decode('idna').encode('utf-8')
                str += ('%s, ' % unicode_domain)
            sys.stdout.write('%s*/\n' % str.rstrip())

            sys.stdout.write('    ')
            str = ''
            for n in value:
                str += ('"%s", ' % n)
            sys.stdout.write('%s\n' % str.rstrip())
        else:
            sys.stdout.write('    ')
            str = ''
            for n in value:
                str += ('"%s", ' % n)
            sys.stdout.write('%s\n' % str.rstrip())

    print '    0'
    print '};'

if __name__ == '__main__':
    main()
