# flattern json


from xml.dom.minidom import parse
# from xml.parsers.expat import ExpatError

import json
# from datetime import datetime

import ijson
import pandas as pd
from pandas.io.json import *
import pymongo, ssl, base64, os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class JsonDoc:

    #Page attribute contains block and table list
    PAGE_ATTR_LIST = ["PageNo", "PageID", "Lang", "XDPI", "YDPI", "PageStartX", "PageStartY", "PageWidth", "PageHeight", \
                      "PrintAreaStartX", "PrintAreaStartY", "PrintAreaWidth", "PrintAreaHeight"]

    # For Block List
    BLOCK_ATTR_LIST = ["BlockID", "BlockType", "BlockStartX", "BlockStartY", "BlockWidth", "BlockHeight", "BlockStyle"]
    BLOCK_LINE_ATTR_LIST = ["LineID", "LineStartX", "LineStartY", "LineWidth", "LineHeight", "LineStyle"]
    BLOCK_WORD_ATTR_LIST = ["WordID", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordStyle"]
    BLOCK_CHAR_ATTR_LIST = ["CharWidth", "CharStartX", "CharStartY", "CharStyle", "CharCN", "CharID", "CharValue",
                      "CharHeight"]

    # For Table List
    TABLE_ATTR_LIST = ["TableID", "TableStartX", "TableStartY", "TableWidth", "TableHeight", "TableRows", "TableCols", "TableStyle"]
    TABLE_ROW_ATTR_LIST = ["RowID", "RowStartX", "RowStartY", "RowWidth", "RowHeight", "RowStyle"]
    TABLE_CELL_ATTR_LIST = ["CellID", "CellRowID", "CellColumnID", "CellStartX", "CellStartY", "CellWidth", "CellHeight", "CellRowSpan", "CellColumnSpan"]
    TABLE_LINE_ATTR_LIST = ["LineID", "LineStartX", "LineStartY", "LineWidth", "LineHeight", "LineStyle"]
    TABLE_WORD_ATTR_LIST = ["WordID","WordValue", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordStyle"]
    TABLE_CHAR_ATTR_LIST = ["CharWidth", "CharStartX", "CharStartY", "CharStyle", "CharCN", "CharID", "CharValue", "CharHeight"]

    def __init__(self, jsonInput):
        with open(jsonInput, 'r') as jsonInputFile:
            objects = ijson.items(jsonInputFile, "Document.item")
            print(objects)
            blocks = list(objects)
            print(blocks)
            jsonInputFile.close()
            self.__rawJson = blocks
            self.__dataFrame = pd.DataFrame()
            self.__response = ""

    def getRawJson(self):
        return self.__rawJson

    def getAttrList(self):

        # pageAttrList, blockAttrList, lineAttrList = {}, {}, {}
        pageAttrList = self.PAGE_ATTR_LIST
        blockAttrList = self.BLOCK_ATTR_LIST
        tableAttrList = self.TABLE_ATTR_LIST
        tableRowAttrList = self.TABLE_ROW_ATTR_LIST
        tableCellAttrList = self.TABLE_CELL_ATTR_LIST
        tableLineAttrList = self.TABLE_LINE_ATTR_LIST
        tableWordAttrList = self.TABLE_WORD_ATTR_LIST
        blockLineAttrList = self.BLOCK_LINE_ATTR_LIST
        blockWordAttrList = self.BLOCK_WORD_ATTR_LIST

        for page in self.getRawJson():
            pageAttrList = list(set(pageAttrList).intersection(set(page.keys())))

            for block in page["BlockList"]:
                blockAttrList = list(set(blockAttrList).intersection(set(block.keys())))
                for line in block["LineList"]:
                    blockLineAttrList = list(set(blockLineAttrList).intersection(set(line.keys())))
                    for wordLine in line["WordList"]:
                        blockWordAttrList = list(set(blockWordAttrList).intersection(set(wordLine.keys())))
            print(blockWordAttrList)
            for table in page["TableList"]:
                tableAttrList = list(set(tableAttrList).intersection(set(table.keys())))
                for row in table["RowList"]:
                    tableRowAttrList=list(set(tableRowAttrList).intersection(set(row.keys())))
                    for cell in row["CellList"]:
                        tableCellAttrList = list(set(tableCellAttrList).intersection(set(cell.keys())))
                        for line in cell["LineList"]:
                            tableLineAttrList = list(set(tableLineAttrList).intersection(set(line.keys())))
                            for wordLine in line["WordList"]:
                                tableWordAttrList = list(set(tableWordAttrList).intersection(set(wordLine.keys())))

        return pageAttrList, blockAttrList, blockLineAttrList, blockWordAttrList, tableAttrList, tableRowAttrList, tableCellAttrList, tableLineAttrList, tableWordAttrList

    def flattenJson(self):

        pageAttrList, blockAttrList, blockLineAttrList, blockWordAttrList, tableAttrList, tableRowAttrList, tableCellAttrList, tableLineAttrList, tableWordAttrList = self.getAttrList()

        blockAttrList = map(lambda x: list(["BlockList", x]), blockAttrList)
        blockLineAttrList = map(lambda x: list(["BlockList", "LineList", x]), blockLineAttrList)
        blockWordAttrList = map(lambda x: list(["BlockList", "LineList", "WordList", x]), blockWordAttrList)
        # print(blockWordAttrList)
        tableAttrList = map(lambda x: list(["TableList", x]), tableAttrList)
        tableRowAttrList = map(lambda x: list(["TableList","RowList", x]), tableRowAttrList)
        tableCellAttrList = map(lambda x: list(["TableList","RowList","CellList", x]), tableCellAttrList)
        tableLineAttrList = map(lambda x: list(["TableList","RowList","CellList","LineList", x]), tableLineAttrList)
        tableWordAttrList = map(lambda x: list(["TableList","RowList","CellList", "LineList", "WordList", x]), tableWordAttrList)

        blockRawDF = json_normalize(self.getRawJson(), ["BlockList", "LineList", "WordList", "CharList"], pageAttrList + blockAttrList  + blockLineAttrList + blockWordAttrList)
        tableRawDF = json_normalize(self.getRawJson(), ["TableList", "RowList", "CellList", "LineList", "WordList", "CharList"], pageAttrList + tableAttrList  + tableRowAttrList + tableCellAttrList + tableLineAttrList + tableWordAttrList)


        blockRawDF.rename(columns=lambda x: x.split(".")[-1], inplace=True)
        tableRawDF.rename(columns=lambda x: x.split(".")[-1], inplace=True)

        # Arrange the columns order
        blockColNames = self.PAGE_ATTR_LIST + self.BLOCK_ATTR_LIST + self.BLOCK_LINE_ATTR_LIST + self.BLOCK_WORD_ATTR_LIST + self.BLOCK_CHAR_ATTR_LIST
        tableColNames = self.PAGE_ATTR_LIST + self.TABLE_ATTR_LIST + self.TABLE_ROW_ATTR_LIST + self.TABLE_CELL_ATTR_LIST + self.TABLE_LINE_ATTR_LIST + self.TABLE_WORD_ATTR_LIST

        blockColNames = [colName for colName in blockColNames if colName in list(blockRawDF.columns.values)]
        tableColNames = [colName for colName in tableColNames if colName in list(tableRawDF.columns.values)]

        blockRawDF = blockRawDF[blockColNames]
        tableRawDF = tableRawDF[tableColNames]
        # print(blockRawDF)
        self.dataFrame = tableRawDF
        print self.dataFrame
        tableRawDF.to_csv('outputTable.csv')

        blockRawDF.to_csv('outputBlock.csv')

        # self.dataFrame = rawDF
        # jsonObject = self.__dataFrame.to_json(orient="records")
        # return jsonObject

    def dataframetoCSV(self):
        self.dataFrame.to_csv('output.csv')

    def groupByPageId(self):
        self.dataFrame = self.dataFrame.set_index(['PageID','BlockID'])
        groups = self.dataFrame.groupby(level='PageID')
        # iterate over all groups
        for key, group in groups:
            print('key: ' + key)
            print('group:')
            print(group)
            print('\n')



if __name__ == "__main__":
    jsonInput = '/Users/badrkhamis/Documents/DataScience/block_analysis/es.json'
    jsonClass = JsonDoc(jsonInput)
    jsonObject = jsonClass.flattenJson()
    jsonClass.dataframetoCSV()
    # jsonClass.groupByPageId()
