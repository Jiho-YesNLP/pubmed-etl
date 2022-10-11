import code
import gzip
from lxml import etree
import glob
import sys
import os
import json
import multiprocessing

from tqdm import tqdm


def convert(f):
    filename, file_ext = os.path.splitext(f)
    json_out = os.path.join(outdir,
                         filename.split(os.sep)[-1].split('.')[0] + '.json')
    if os.path.exists(json_out):
        os.remove(json_out)
    with gzip.open(f) if file_ext == '.gz' else open(f) as fh, \
            open(json_out, 'a') as out_fh:
        context = etree.parse(fh)
        doc = {}
        for cit in context.iter("MedlineCitation"):
            # PMID
            doc['PMID'] = cit.xpath('PMID/text()')[0]
            # Date
            pubdate = None
            if cit.xpath('DateCompleted'):
                ymd = [elm.text for elm in
                       cit.xpath('DateCompleted')[0].getchildren()]
                pubdate = "{}-{}-{}".format(*ymd)
            doc['PublishedDate'] = pubdate
            # MeshHeadings
            doc['MeSH'] = \
                cit.xpath('MeshHeadingList/MeshHeading/DescriptorName/@UI')
            # Title
            try:
                doc['Title'] = cit.xpath('Article/ArticleTitle/text()')[0]
            except IndexError as e:
                doc['Title'] = "No Title"
            # Authors
            authors = []
            for p in cit.xpath('Article/AuthorList/Author'):
                name = ''
                if p.xpath('LastName'):
                    name += p.xpath('LastName')[0].text
                if p.xpath('ForeName'):
                    name = p.xpath('ForeName')[0].text + ' ' + name
                if p.xpath('CollectiveName'):
                    name = p.xpath('CollectiveName')[0].text
                authors.append(name)
            doc['Authors'] = authors
            # Abstract
            doc['Abstract'] = ' '.join(
                cit.xpath('Article/Abstract/AbstractText/text()'))
            # Keywords
            doc['Keywords'] = cit.xpath('Article/KeywordList/Keyword/text()')
            # Journal
            doc['Journal'] = cit.xpath('Article/Journal/Title/text()')[0]
            json.dump({'index': {'_index': 'pubmed', '_id': doc['PMID']}},
                      out_fh)
            out_fh.write('\n')
            json.dump(doc, out_fh)
            out_fh.write('\n')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("xml2json [input_dir] [output_dir]")
        sys.exit(1)
    indir, outdir = sys.argv[1:]
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    files = [glob.glob(os.path.join(indir, e)) for e in ['*.xml', '*.gz']]
    files = [file for sublist in files for file in sublist]
    with multiprocessing.Pool() as pool:
        r = list(tqdm(pool.imap(convert, files), total=len(files)))



# code.interact(local=dict(locals(), **globals()))
