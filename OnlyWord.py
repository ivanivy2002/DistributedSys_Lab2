import json
import math
import os
from collections import Counter

from mrjob.job import MRJob
import jieba
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

import MyJSONOutputProtocol


class OnlyWord(MRJob):
    # OUTPUT_PROTOCOL = MyJSONOutputProtocol.MyJSONOutputProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # 定义mapper
    def mapper(self, _, line):
        docno, contents = line.split('\t', 1)
        seg_list = (jieba.cut_for_search(contents))
        for word in seg_list:
            if len(word) > 1:
                yield word, docno
    def reducer(self, word, values):
        values = list(values)
        counter = Counter()
        for docno in values:
            counter[docno] += 1
        num = len(counter)
        out_list = [word, num]
        yield word, str(out_list)  # ! 不能直接输出列表, 否则会报错

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)
                ]


if __name__ == '__main__':
    output_dir = 'outputs/onlyword'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    input_file = 'data/document_utf8.txt'
    job = OnlyWord(args=[input_file, '--output-dir', output_dir])
    with job.make_runner() as runner:
        runner.run()
