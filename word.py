import pandas as pd
import numpy as np
import ijson
import json, re, copy
class Title:
    def __init__(self,path):
        data = pd.read_csv(path)
        self.__data = data

    def Font_size(self):
        self.__data = self.__data[self.__data['PageNo'] == 0]
        self.__data['FontSize'].fillna(1,inplace=True)
        fontlist = []
        for font in self.__data['FontSize']:
            if font >= 0 and font <= self.__data['FontSize'].quantile([.25]).get_values():
                fontlist.append(1)
            elif font > self.__data['FontSize'].quantile([.25]).get_values() and font <= self.__data['FontSize'].quantile(
                    [.5]).get_values():
                fontlist.append(2)
            elif font > self.__data['FontSize'].quantile([.5]).get_values() and font <= self.__data['FontSize'].quantile(
                    [.75]).get_values():
                fontlist.append(3)
            elif font > self.__data['FontSize'].quantile([.75]).get_values() and font <= self.__data['FontSize'].quantile(
                    [.9]).get_values():
                fontlist.append(4)
            else:
                fontlist.append(6)
        return fontlist
    def Top_position(self):
        topposition = []
        self.__data = self.__data[self.__data['PageNo']==0]
        for row in self.__data['WordStartY']:
            if row > 0 and row <= self.__data['WordStartY'].quantile([.25]).get_values():
                topposition.append(5)
            elif row > self.__data['WordStartY'].quantile([.25]).get_values() and row <= self.__data['WordStartY'].quantile(
                    [.4]).get_values():
                topposition.append(4)
            elif row > self.__data['WordStartY'].quantile([.4]).get_values() and row <= self.__data['WordStartY'].quantile(
                    [.5]).get_values():
                topposition.append(2)
            elif row > self.__data['WordStartY'].quantile([.6]).get_values() and row <= self.__data['WordStartY'].quantile(
                    [.7]).get_values():
                topposition.append(1)
            else:
                topposition.append(0)
        return topposition
    def Center_Position(self):
        topposition = []
        self.__data = self.__data[self.__data['PageNo'] == 0]
        for row in self.__data['WordStartX']:

            if row > 0 and row <= self.__data['WordStartX'].quantile([.25]).get_values():
                topposition.append(3)
            elif row > self.__data['WordStartX'].quantile([.25]).get_values() and row <= self.__data['WordStartX'].quantile(
                    [.60]).get_values():
                topposition.append(5)
            else:
                topposition.append(1)

        return topposition
    def Font_bold(self):
        self.__data = self.__data[self.__data['PageNo'] == 0]
        listb = []
        for tr in self.__data['FontWeight']:
            if str(tr).strip() == "bold":
                listb.append(1)
            else:
                listb.append(0)
        return listb
    def implement(self):
        self.__data = self.__data[self.__data['PageNo'] == 0]
        self.__data['font_size'] = self.Font_size()
        print self.__data.head()
        self.__data['topposition'] = self.Top_position()
        self.__data['centerposition'] = self.Center_Position()
        self.__data['bolding']= self.Font_bold()
        self.__data['Title_Ratio'] = self.__data['font_size']+self.__data['topposition']+self.__data['centerposition']+self.__data['bolding']
        print self.__data.head(10)
        # return self.__data[self.__data['WordDistance'].quantile([.2]).get_values()]
        # for index , dat in self.__data.iterrows():
        #
        #     print dat['Title_Ratio']
        wordvalue = " "
        # self.__data = self.__data['Title_Ratio'].max()
        self.__data= self.__data.nlargest(1,'Title_Ratio')
        self.__data = self.__data[self.__data['TextGroupSeparator'] == False]


        print self.__data['WordValue']
        m = 0
        dic = {}
        word_dic = {}
        lis = []
        s= 1
        for index, word in self.__data.iterrows():
            if word['LineStartY']== m:

                 wordvalue = wordvalue.join([' ', ' ' + word['WordValue']])
                 dic['title'+str(s)] = wordvalue.strip()
            else:
                wordvalue = " "
                s += 1
                wordvalue = wordvalue.join([' ', ' ' + word['WordValue']])
                dic['title' + str(s)] = wordvalue.strip()
                m = word['LineStartY']
                # print m , word['LineStartY']


        # self.__data=self.__data[self.__data['TextGroupSeparator'] == False]

        # dic['title'] = wordvalue.strip()
        word_dic['Titles']=dic.values()
        lis.append(word_dic)


        with open("title.json", "w") as jsonFile:
            json.dump(word_dic, jsonFile)
        # return self.__data[['WordValue','Title_Ratio']]


        # return self.__data.LineText.drop_duplicates()
if __name__ == ("__main__"):
    path = '/Users/badrkhamis/Desktop/python_code/blocks_analysis/reg_form.csv'
    objects = Title(path)
    sd = objects.implement()
    print sd

'''tax - sin - workpermit - accountonformation  '''
