import pandas as pd
import numpy as np
import json, os

class Title:
    def __init__(self,path):
        data = pd.read_csv(path)
        self.__data = data
        # print self.__data['TextGroupSeparator'].dtypes
    def Font_size(self):
        self.__data['FontSize'].fillna(1,inplace=True)
        fontlist = []
        t= self.__data['FontSize'].max()
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
                    [.8]).get_values():
                fontlist.append(4)
            elif font > self.__data['FontSize'].quantile([.8]).get_values() and font <= self.__data[
                    'FontSize'].quantile(
                    [.90]).get_values():
                fontlist.append(4.5)
            elif font > self.__data['FontSize'].quantile([.9]).get_values() and font <= self.__data[
                    'FontSize'].quantile(
                    [.99]).get_values():
                fontlist.append(5)
            else:
                fontlist.append(6)

        return fontlist
    def extra_fetures(self):
        extra=[]
        topposition =[]
        for index,value in self.__data.iterrows():

            if value['LineStartX'] >= self.__data['LineStartX'].quantile([.25]).get_values() and value['LineStartX'] <= self.__data['LineStartX'].quantile(
                    [.73]).get_values() and value['FontSize'] ==self.__data['FontSize'].max() :
                extra.append(1)
            else:
                extra.append(0)
        return extra
    def Top_position(self):
        topposition = []
        for row in self.__data['LineStartY']:
            if row > 0 and row <= self.__data['LineStartY'].quantile([.25]).get_values():
                topposition.append(5)
            elif row > self.__data['LineStartY'].quantile([.25]).get_values() and row <= self.__data['LineStartY'].quantile(
                    [.4]).get_values():
                topposition.append(4)
            elif row > self.__data['LineStartY'].quantile([.4]).get_values() and row <= self.__data['LineStartY'].quantile(
                    [.5]).get_values():
                topposition.append(2)
            elif row > self.__data['LineStartY'].quantile([.6]).get_values() and row <= self.__data['LineStartY'].quantile(
                    [.7]).get_values():
                topposition.append(1)
            else:
                topposition.append(0)
        return topposition
    def Center_Position(self):
        topposition = []
        for row in self.__data['LineStartX']:

            if row > 0 and row <= self.__data['LineStartX'].quantile([.25]).get_values():
                topposition.append(3)
            elif row > self.__data['LineStartX'].quantile([.25]).get_values() and row <= self.__data['LineStartX'].quantile(
                    [.73]).get_values():
                topposition.append(5)
            else:
                topposition.append(1)

        return topposition
    def Font_bold(self):
        listb = []
        for tr in self.__data['FontWeight']:
            if str(tr).strip() == "bold":
                listb.append(1)
            else:
                listb.append(0)
        return listb
    def implement(self):
        title = {}
        self.__data['extre'] = self.extra_fetures()
        self.__data['fontposition'] = self.Font_size()
        self.__data['topposition'] = self.Top_position()
        self.__data['centerposition'] = self.Center_Position()
        self.__data['bolding']= self.Font_bold()
        self.__data['Title_Ratio'] = self.__data['fontposition']+self.__data['topposition']+self.__data['centerposition']+self.__data['bolding']+self.__data['extre']
        self.__data.to_csv('cls.csv')
        self.__data= self.__data.nlargest(1,'Title_Ratio').drop_duplicates()
        # self.__data=self.__data[self.__data['TextGroupSeparator'] == False]
        # return self.__data[['WordValue','LineText','Title_Ratio']].drop_duplicates()
        # print self.__data.LineText.drop_duplicates()
        # self.__data = self.__data['LineText'].drop_duplicates()
        print self.__data['LineText'].values[0].strip()
        title['Title']=self.__data['LineText'].values[0].strip()
        # print title
        return title

if __name__ == ("__main__"):
    path = '/Users/badrkhamis/Desktop/python_code/blocks_analysis/reg_form.csv'
    objects = Title(path)
    sd = objects.implement()
    filename = 'data.xml.json'
    with open(filename, 'r+') as f:
        data = json.load(f)
        tmp = data["Document"]
        data["Document"].append(sd)
    data["Document"].append({'Semantic Document Classification': ''})
    data["Document"].append({'Semantic Percentage Match Document Classification': ''})
    #
    os.remove(filename)
    with open(filename, 'w') as f:
        f.seek(0)  # rewind

        json.dump(data, f, indent=4)
        f.truncate()


'''tax - sin - workpermit - accountonformation  '''
