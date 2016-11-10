# -*- coding: utf-8 -*-
__author__ = 'trungtv'
import codecs
import re
import string
import pickle
import sklearn
import sklearn_crfsuite
import os


class ViTokenizer:
    bi_grams = set()
    tri_grams = set()
    with codecs.open(os.path.join(os.path.dirname(__file__), 'words.txt'), 'r', encoding='utf-8') as fin:
        for token in fin.read().split('\n'):
            tmp = token.split(' ')
            if len(tmp) == 2:
                bi_grams.add(token)
            elif len(tmp) == 3:
                tri_grams.add(token)
    with open(os.path.join(os.path.dirname(__file__), 'pyvi.pkl'), 'rb') as fin:
        model = pickle.load(fin)

    @staticmethod
    def word2features(sent, i, is_training):
        word = sent[i][0] if is_training else sent[i]

        features = {
            'bias': 1.0,
            'word.lower()': word.lower(),
            #   'word[-3:]': word[-3:],
            #   'word[-2:]': word[-2:],
            'word.isupper()': word.isupper(),
            'word.istitle()': word.istitle(),
            'word.isdigit()': word.isdigit(),
        }
        if i > 0:
            word1 = sent[i - 1][0] if is_training else sent[i - 1]
            features.update({
                '-1:word.lower()': word1.lower(),
                '-1:word.istitle()': word1.istitle(),
                '-1:word.isupper()': word1.isupper(),
                '-1:word.bi_gram()': ' '.join([word1, word]).lower() in ViTokenizer.bi_grams,
            })
            if i > 1:
                word2 = sent[i - 2][0] if is_training else sent[i - 2]
                features.update({
                    '-2:word.tri_gram()': ' '.join([word2, word1, word]).lower() in ViTokenizer.tri_grams,
                })
                #    else:
                #        features['BOS'] = True

        if i < len(sent) - 1:
            word1 = sent[i + 1][0] if is_training else sent[i + 1]
            features.update({
                '+1:word.lower()': word1.lower(),
                '+1:word.istitle()': word1.istitle(),
                '+1:word.isupper()': word1.isupper(),
                '+1:word.bi_gram()': ' '.join([word, word1]).lower() in ViTokenizer.bi_grams,
            })
            if i < len(sent) - 2:
                word2 = sent[i + 2][0] if is_training else sent[i + 2]
                features.update({
                    '+2:word.tri_gram()': ' '.join([word, word1, word2]).lower() in ViTokenizer.tri_grams,
                })
                #    else:
                #        features['EOS'] = True

        return features

    @staticmethod
    def sent2features(sent, is_training):
        return [ViTokenizer.word2features(sent, i, is_training) for i in range(len(sent))]

    # @staticmethod
    # def sylabelize(text):
    #     # print type(text)
    #     tmp = re.findall(r"((\d+[\.,-_]*\d*)+|\w+|[^\w\s])", text, re.UNICODE)
    #     # tmp = re.findall(r"(\w+|[^\w\s])", text, re.UNICODE)

    #     # print tmp
    #     return [a[0] for a in tmp]


    @staticmethod
    def sylabelize(text):
        ret = []
        lines = text.splitlines(True)
        for line in lines:
            parts = re.split('\s', line)
            for p in parts:
                sub_parts = re.split('((?<!\d)[,.](?!\d)|\t|\n)',p)
                ret.extend(sub_parts)
            ret.append("\n")
        return ret


    @staticmethod
    def tokenize(str):
        tmp = ViTokenizer.sylabelize(str)
        labels = ViTokenizer.model.predict([ViTokenizer.sent2features(tmp, False)])
        
        output = tmp[0]
        for i in range(1, len(labels[0])):
            # print "token __"+tmp[i]+"__ label " + labels[0][i]
            if labels[0][i] == 'I_W' and tmp[i] not in string.punctuation and\
                            tmp[i-1] not in string.punctuation and\
                    not tmp[i][0].isdigit() and not tmp[i-1][0].isdigit():
                output = output + '_' + tmp[i]
            elif tmp[i] in string.punctuation:
                output = output + tmp[i]
            else:
                output = output + ' ' + tmp[i]
        return output


class ViPosTagger:
    filtered_tags = set(string.punctuation)
    filtered_tags.add(u'\u2026')
    filtered_tags.add(u'\u201d')
    filtered_tags.add(u'\u201c')
    filtered_tags.add(u'\u2019')
    filtered_tags.add('...')

    with open(os.path.join(os.path.dirname(__file__), 'pyvipos.pkl'), 'rb') as fin:
        model = pickle.load(fin)

    @staticmethod
    def word2features(sent, i, is_training):
        word = sent[i][0] if is_training else sent[i]

        features = {
            'bias': 1.0,
            'word.lower()': word.lower(),
            # 'word.isupper()': word.isupper(),
            'word.istitle()': word.istitle(),
            'word.isdigit()': word.isdigit(),
            'word[:1].isdigit()': word[:1].isdigit(),
            'word[:3].isupper()': word[:3].isupper(),
            #       'word.indict()': word in vi_words,
            'word.isfiltered': word in ViPosTagger.filtered_tags,
        }
        if i > 0:
            word1 = sent[i - 1][0] if is_training else sent[i - 1]
            features.update({
                '-1:word.lower()': word1.lower(),
                '-1:word.istitle()': word1.istitle(),
                '-1:word[:1].isdigit()': word1[:1].isdigit(),
                '-1:word[:3].isupper()': word1[:3].isupper(),
            })
            if i > 1:
                word2 = sent[i - 2][0] if is_training else sent[i - 2]
                features.update({
                    '-2:word.lower()': word2.lower(),
                    '-2:word.istitle()': word2.istitle(),
                    '-2:word.isupper()': word2.isupper(),
                })
        else:
            features['BOS'] = True

        if i < len(sent) - 1:
            word1 = sent[i + 1][0] if is_training else sent[i + 1]
            features.update({
                '+1:word.lower()': word1.lower(),
                '+1:word.istitle()': word1.istitle(),
                '+1:word[:1].isdigit()': word1[:1].isdigit(),
                '+1:word.isupper()': word1.isupper(),
            })
            if i < len(sent) - 2:
                word2 = sent[i + 2][0] if is_training else sent[i + 2]
                features.update({
                    '+2:word.lower()': word2.lower(),
                    '+2:word.istitle()': word2.istitle(),
                    '+2:word.isupper()': word2.isupper(),
                })
        else:
            features['EOS'] = True

        return features

    @staticmethod
    def sent2features(sent, is_training):
        return [ViPosTagger.word2features(sent, i, is_training) for i in range(len(sent))]

    @staticmethod
    def postagging(str):
        tmp = str.split(' ')
        labels = ViPosTagger.model.predict([ViPosTagger.sent2features(tmp, False)])
        #for i in range(len(labels[0])):
        #    print tmp[i], labels[0][i]
        return tmp, labels[0]



if __name__ =="__main__":
    ViTokenizer.tokenize_file("/home/frog/Desktop/dev/word-procs/tokenizing/fb_data_sent.json", num_worker=3)