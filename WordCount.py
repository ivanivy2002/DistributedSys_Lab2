import json
import math
import os
from collections import Counter

from mrjob.job import MRJob
import jieba
from mrjob.protocol import RawValueProtocol
from mrjob.step import MRStep

import MyJSONOutputProtocol


class WordCount(MRJob):
    # OUTPUT_PROTOCOL = MyJSONOutputProtocol.MyJSONOutputProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # 定义mapper
    def mapper(self, _, line):
        docno, contents = line.split('\t', 1)
        # proj = json.loads(line)
        # docno = proj["docno"]
        # contents = proj["contenttitle"] + " " + proj["content"]
        seg_list = (jieba.cut_for_search(contents))
        for word in seg_list:
            if len(word) > 1:
                yield docno, word

    def reducer_tf(self, docno, words):
        # 对每个文档执行
        words = list(words)
        word_counter = Counter()  # 统计每个 word 在一个文档中出现的次数
        for word in words:
            word_counter[word] += 1
        word_per_doc = sum(cnt for _, cnt in word_counter.items())  # word_per_doc 为一个文档中所有词的总数
        for word, cnt in word_counter.items():
            tf = cnt / word_per_doc  # tf 为词频
            yield None, (word, docno, tf)

    def reducer_doc_num(self, _, values):
        # 对所有记录执行
        values = list(values)  # 将生成器转换为列表, 以便多次使用
        doc_counter = Counter()  # 统计文档总数
        for word, docno, tf in values:
            if docno not in doc_counter:
                doc_counter[docno] = 1
        doc_num = len(doc_counter)  # doc_num 为文档总数
        for word, docno, tf in values:
            yield word, (docno, tf, doc_num)

    def reducer_idf(self, word, values):
        # 对每个词执行
        values = list(values)
        # 统计每个 word 在所有文档中出现的次数
        word_in_doc_counter = Counter()
        for docno, _, _ in values:
            word_in_doc_counter[docno] += 1
        word_in_doc_num = len(word_in_doc_counter)
        idf = math.log(values[0][2] / word_in_doc_num + 1)
        for docno, tf, _ in values:
            tf_idf = tf * idf
            yield word, (docno, tf_idf)

    def reducer_out(self, word, values):
        values = list(values)
        out_list = [word, values]
        yield word, str(out_list)  # ! 不能直接输出列表, 否则会报错

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer_tf)
            , MRStep(reducer=self.reducer_doc_num)
            , MRStep(reducer=self.reducer_idf)
            , MRStep(reducer=self.reducer_out)
                ]


if __name__ == '__main__':
    output_dir = 'outputs/wordcount'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    input_file = 'data/document_utf8.txt'
    job = WordCount(args=[input_file, '--output-dir', output_dir])
    with job.make_runner() as runner:
        runner.run()
