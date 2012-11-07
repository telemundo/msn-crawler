#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, time, codecs, yaml, requests
from optparse import OptionParser
from BeautifulSoup import BeautifulSoup
from random import choice

parser = OptionParser(usage='usage: %prog [options] output')
parser.add_option('-v', action='count', dest='verbosity', default=0, help='increase output verbosity')
parser.add_option('-q', '--quiet', action='store_true', dest='quiet', help='hide all output')
parser.add_option('-c', '--config', dest='config', default='config.yaml', type='string', help='YAML configuration file (default: config.yaml)')
parser.add_option('-o', '--offset', dest='offset', default=100, type='int', help='number of records to fetch per page (default: 100)')
parser.add_option('-l', '--limit', dest='limit', default=200000, type='int', help='number of records to fetch per search (default: 10,000)')
parser.add_option('-p', '--pause', dest='pause', default=5, type='int', help='number of seconds to wait between pagination request (default: 5)')
(options, args) = parser.parse_args()
uniq_list = []

def load_config(filename):
    ''' Loads and validates the configuration file data '''
    fh = open(filename)
    config = yaml.load(fh)
    fh.close()
    if 'api' not in config:
        parser.error('%s is missing the "api" tree root' % filename)
    if 'endpoint' not in config['api']:
        parser.error('%s is missing the "api/endpoint" subtree' % filename)
    if 'search' not in config['api']:
        parser.error('%s is missing the "api/search" subtree' % filename)

    return config

def headers(args):
    ''' Adds the custom User-Agent to the request headers '''
    if args.get('headers') is None:
        args['headers'] = dict()
    args['headers'].update({ 'User-Agent':'msnfetch/1.1 (+http://support.tlmdservices.com/)' })

    return args

def search(searchurl, searchdata, output):
    page = 0
    records = 0
    hooks = dict(args=headers)
    if options.verbosity >= 3 and not options.quiet:
        print '[%s] DEBUG: executing %s' % (time.strftime('%Y-%m-%d %H:%M:%S'), searchdata)
    while records < options.limit:
        if options.verbosity >= 2 and not options.quiet:
            print '[%s] INFO: fetching page %d' % (time.strftime('%Y-%m-%d %H:%M:%S'), page+1)
        searchdata['ind'] = (page * options.offset)
        pagerequest = requests.get(searchurl, hooks=hooks, params=searchdata)
        if options.verbosity >= 3 and not options.quiet:
            print '[%s] DEBUG: %s (%d)' % (time.strftime('%Y-%m-%d %H:%M:%S'), pagerequest.url, pagerequest.status_code)
        pagesoup = BeautifulSoup(pagerequest.content)
        pagevideos = pagesoup('video')
        if len(pagevideos) > 0:
            for pagevideo in pagevideos:
                uuid = pagevideo('uuid')[0]
                title = pagevideo('title')[0]
                refid = uuid['ref']
                if not any(refid == x for x in uniq_list):
                    output.write(u'%s\t%s\t%s\n' % (uuid.contents[0], refid, title.contents[0]))
                    uniq_list.append(refid)
                    records += 1
                if records == options.limit:
                    if options.verbosity >= 2 and not options.quiet:
                        print '[%s] INFO: max number of records reached' % (time.strftime('%Y-%m-%d %H:%M:%S'))
                    return records
            if options.verbosity >= 2 and not options.quiet:
                print '[%s] INFO: sleeping for %d seconds' % (time.strftime('%Y-%m-%d %H:%M:%S'), options.pause)
            time.sleep(options.pause)
            page += 1
        else:
            if options.verbosity >= 1 and not options.quiet:
                print '[%s] NOTICE: no more records found' % (time.strftime('%Y-%m-%d %H:%M:%S'))
            return records

    return records

def main():
    if len(args) < 1:
        parser.error('you must specify the output file.')
    filename = args[0]
    if not os.path.exists(options.config):
        parser.error('the configuration file %s does not exist.' % options.config)
    config = load_config(options.config)
    basedict = {
        'sf': 'ActiveStartDate', 'sd': -1, 'ps': options.offset
    }
    records = 0
    if len(config['api']['search']) > 0:
        fp = codecs.open(filename, 'w', 'utf-8')
        for searchdata in config['api']['search']:
            endpoint = choice(config['api']['endpoint'])
            searchurl = endpoint.rstrip('/') + '/' + searchdata['method']
            searchdict = dict(basedict.items() + searchdata['params'].items())
            records += search(searchurl, searchdict, fp)
            if options.verbosity >= 2 and not options.quiet:
                print '[%s] INFO: sleeping for %d seconds' % (time.strftime('%Y-%m-%d %H:%M:%S'), options.pause)
            time.sleep(options.pause)
        if options.verbosity >= 1 and not options.quiet:
            print '[%s] NOTICE: downloaded %d records' % (time.strftime('%Y-%m-%d %H:%M:%S'), records)
        fp.close()
    else:
        if not options.quiet:
            print '[%s] WARNING: there are no search entries in the configuration file' % (time.strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    main()