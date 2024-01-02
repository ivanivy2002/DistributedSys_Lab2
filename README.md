# DistributedSys_Lab2

# 项目目录和主题程序设计
![[Pasted image 20231206223134.png|239]]

![[Pasted image 20231206221546.png|231]]
以下做简单介绍，详细内容见代码
## preprocess.py
预处理程序
处理 data/document.dat 来输出 document_utf8.txt
![[Pasted image 20231206194103.png|525]]
格式是
docno \\t title \\t content
文章号 \\t 标题 \\t 内容
## WordCount.py
主程序
1. mapper 对每个文档的每个字生成键值对
2. reducer_tf 计算词频
3. reducer_doc_num 统计文档数量，用来下一步计算idf
4. reducer_doc_idf 计算idf 和tf_idf
5. reducer_out 输出\[word, \[docno1, tf_idf1],\[docno2, tf_idf2],...] 
```python
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
             
```

主函数重新定义输入输出的格式, 不用命令行输出, 命令行输出很容易有编码错误! 
输出到part-xxxxx
![[Pasted image 20231206212555.png|500]]
输出的part-xxxxx文件形如: 
![[Pasted image 20231206212724.png|500]]

## findWord.py
按照用户输入的关键字寻找文档, 输出docno, title, tf_idf， 或者docno, url, title, tf_idf
tips: 寻找url是进入json找, 我在preprocess输出了json, 两种寻找我都提供了，一种是根据json找，一种是根据txt找，没有url

docno, title, tf_idf
![[Pasted image 20231206193213.png|525]]

docno, url, title, tf_idf
![[Pasted image 20231206215317.png]]



# 其他程序

## OnlyWord.py
仅仅输出单词和单词的出现文档数量，前期调试和查看用的
![[Pasted image 20231206221704.png|173]]
# 感想和bug

## 分词问题
一般来说现代汉语的词汇是二字的，并且为了防止单字符的干扰，我设置了len>1的筛查。
![[Pasted image 20231206220027.png|232]]
但是存在单字比如鲸，鳄，猫，狗。
![[Pasted image 20231206220016.png]]
那么或许可以做unicode范围检查。但是我发现汉字虽然是\u4e00到\u9fff，却有一些全角符号落在其中，其实很难办，为了简化问题，才做了上面的len>1的检查。
所以我的查找必须要求大于1.
![[Pasted image 20231206222358.png|525]]

## 编码问题
最大最折腾的问题
1. 程序x的输出和x的后继程序的输入的编码必须一致。
2. json的读取貌似utf-8比较合适，所以我用了utf-8做了整个，仅仅给我们的数据dat是gbk的

## 理解
mapper是并行的，本质可以在多个机器上运行。分出的多个part非常好体现了这一点，让我回味lab1 hhh
## 其他和参考

文字与unicode编码转换网站
[文字与Unicode相互转换在线工具,在线计算,在线计算器,计算器在线计算 (osgeo.cn)](https://www.osgeo.cn/app/sa906)
参考博客
[【MapReduce】使用MapReduce实现TF-IDF算法_mapreduce分词添加tf-idf-CSDN博客](https://blog.csdn.net/heiren_a/article/details/115442096)
但实际我的实现没有很按照他，基本上自己想想也就出来了
是不是还能把我的后边两个reducer阶段合并一下呢？我觉得可以，但是不太有这个必要。
