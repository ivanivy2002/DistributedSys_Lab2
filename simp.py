import json
import os

from mrjob.job import MRJob


class MyJSONOutputProtocol(object):
    def read(self, line):
        k_str, v_str = line.split('\t', 1)
        return json.loads(k_str), json.loads(v_str)

    def write(self, key, value):
        return ('%s\t%s' % (json.dumps(key, ensure_ascii=False), json.dumps(value, ensure_ascii=False))).encode(
            'utf-8')


class Simp(MRJob):
    OUTPUT_PROTOCOL = MyJSONOutputProtocol

    def mapper(self, _, line):
        proj = json.loads(line)
        docno = proj["docno"]
        contenttitle = proj["contenttitle"]
        content = proj["content"]
        yield docno, content

    def reducer(self, docno, contents):
        contents = list(contents)
        yield docno, contents


if __name__ == '__main__':
    output_dir = 'outputs/wordcount'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    input_file = 'data/wordcount_data.txt'
    job = Simp(args=[input_file, '-r', 'local', '--output-dir', output_dir])
    Simp.run()
