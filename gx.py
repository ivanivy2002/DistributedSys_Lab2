"""
@Description: TF-IDF文档生成
@Author: cgx
@File: TF-IDF.py
"""
import json
from collections import Counter
import math

import jieba
from mrjob.job import MRJob
from mrjob.step import MRStep

class MyJSONOutputProtocol(object):
    """
    自定义输出协议，补充了json的中文输出ensure_ascii=False
    """
    def read(self, line):
        k_str, v_str = line.split('\t', 1)
        return json.loads(k_str), json.loads(v_str)

    def write(self, key, value):
        return ('%s\t%s' % (json.dumps(key,ensure_ascii=False), json.dumps(value,ensure_ascii=False))).encode('utf-8')

class TFIDF(MRJob):
        # 设置自定义输出协议
    OUTPUT_PROTOCOL = MyJSONOutputProtocol

    def mapper_extract_content(self,_,line):
        # 提取标题和内容
        doc_and_body = json.loads(line)
        doc = doc_and_body["title"]
        body = doc_and_body["body"]
        seg_list = jieba.cut_for_search(body)
        for word in seg_list:
            yield (doc,(word,1))

    def reducer_get_tf(self, doc, word_count):
            # 按照文件统计词频
        doc_counter = Counter()
        for word,count in word_count:
            doc_counter[word] += count
        total_words = sum(count for _,count in doc_counter.items())
        for word,count in doc_counter.items():
            tf = count/total_words
            yield (None,(word,doc,tf))
       
    def reducer_get_total_doc_number(self, _, word_doc_tfs):
            # 统计所有的doc数目
        doc_counter = Counter()
       
        word_doc_tfs = list(word_doc_tfs)
        for _,doc,_ in word_doc_tfs:
                if doc not in doc_counter:
                    doc_counter[doc] = 1
        doc_num = len(doc_counter)

        for word,doc,tf in word_doc_tfs:
                yield (word,(doc,tf,doc_num))

    def reducer_get_tfidf(self, word, doc_tf_docnum):
            # 计算TF-IDF
        doc_tf_docnum = list(doc_tf_docnum)
        occurence_number = len(doc_tf_docnum)

        results = []
        for doc,tf,doc_num in doc_tf_docnum:
            idf = math.log(doc_num/(occurence_number+1))
            tf_idf = tf*idf
            results.append((doc,tf_idf))
        yield (word,results)