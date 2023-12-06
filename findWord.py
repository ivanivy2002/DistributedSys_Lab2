# -*- coding: gb18030 -*-
import ast
import os

def find(query_word):
    output_dir = 'outputs/wordcount'
    data_dir = 'data/document_utf8.txt'
    file_list = [part_dir for part_dir in os.listdir(output_dir) if part_dir.startswith('part')]
    found = False
    for part_dir in file_list:
        with open(os.path.join(output_dir, part_dir), 'r', encoding='utf-8') as f:
            for line in f:
                # �ҹؼ���
                line_list = ast.literal_eval(line)
                word = line_list[0]
                # �ҵ��ؼ���
                if word == query_word:
                    doc_list = line_list[1]
                    found = True
                    # ���������ص��ĵ�
                    for docno, tf_idf in doc_list:
                        with open(data_dir, 'r', encoding='utf-8') as document:
                            for doc_line in document:
                                doc_line_docno, doc_line_title, doc_line_content = doc_line.split('\t')
                                if doc_line_docno == docno:
                                    print(str(docno) + '\t' + str(doc_line_title).strip() + '\t' + str(tf_idf))

                if found:
                    break
        if found:
            break
    return


if __name__ == '__main__':
    user_input = ''
    while user_input != '!':
        user_input = input("������Ҫ��Ĺؼ��ʣ�")
        find(user_input)
