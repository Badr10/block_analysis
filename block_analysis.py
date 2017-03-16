
import sys
import pandas as pd
from pandas.io.json import *

from xml.dom.minidom import parse
import json
import ijson
import pandas as pd
from pandas.io.json import *
import os, ssl, base64,pymongo
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class JsonDoc:
    URL = "https://gateway-a.watsonplatform.net/calls"
    # API_KEY = "f2a8e8ef27202773e7c846180c42d9c915a5d10e"
    API_KEY = "d7971e9755532c3388cdf0c7f476de6e8c5ec1a5"
    SENTIMENT_ANALYSIS = 1
    MAX_ITEM_COUNT = 5

    PAGE_ATTR_LIST = ["PageNo", "PageID", "Lang", "XDPI", "YDPI", "PageStartX", "PageStartY", "PageWidth", "PageHeight", \
                      "PrintAreaStartX", "PrintAreaStartY", "PrintAreaWidth", "PrintAreaHeight"]
    BLOCK_ATTR_LIST = ["BlockID", "BlockType", "BlockStartX", "BlockStartY", "BlockWidth", "BlockHeight", "BlockStyle"]
    LINE_ATTR_LIST = ["LineID", "LineStartX", "LineStartY", "LineWidth", "LineHeight", "LineStyle"]

    TABLE_ATTR_LIST = ["TableID", "TableStartX", "TableStartY", "TableWidth", "TableHeight", "TableRows", "TableCols",
                       "TableStyle"]
    ROW_ATTR_LIST = ["RowID", "RowStartX", "RowStartY", "RowWidth", "RowHeight", "RowStyle"]
    CELL_ATTR_LIST = ["CellID", "CellRowID", "CellColumnID", "CellStartX", "CellStartY", "CellWidth", "CellHeight",
                      "CellRowSpan", "CellColumnSpan"]

    WORD_ATTR_LIST = ["WordID", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordStyle"]

    def __init__(self, jsonInput):
        with open(jsonInput, 'r') as jsonInputFile:
            # self.jsonDoc = json.load(jsonInputFile.read())
            # self.jsonDoc = json.load(jsonInputFile)
            objects = ijson.items(jsonInputFile, "Document.item")
            blocks = list(objects)
            # testBlock = list(ijson.items(jsonInputFile, "Document.item"))

        jsonInputFile.close()

        self.__rawJson = blocks
        self.__dataFrame = pd.DataFrame()
        # self.__text = ""
        self.__response = ""

    def getRawJson(self):
        return self.__rawJson

    def getAttrList(self):
        # pageAttrList, blockAttrList, lineAttrList = {}, {}, {}
        pageAttrList = self.PAGE_ATTR_LIST
        blockAttrList = self.BLOCK_ATTR_LIST
        lineAttrList = self.LINE_ATTR_LIST

        # for page in self.getRawJson()["Document"]:
        for page in self.getRawJson():
            pageAttrList = list(set(pageAttrList).intersection(set(page.keys())))

            for block in page["BlockList"]:
                blockAttrList = list(set(blockAttrList).intersection(set(block.keys())))

                for line in block["LineList"]:
                    lineAttrList = list(set(lineAttrList).intersection(set(line.keys())))

        return pageAttrList, blockAttrList, lineAttrList

    def getWordAttrList(self):
        wordAttrList = self.WORD_ATTR_LIST

        # for page in self.getRawJson():
        # for block in page["BlockList"]:
        # for line in block["LineList"]:
        # lineAttrList = list(set(lineAttrList).intersection(set(line.keys())))
        # Return the original word attribute list for now, maybe need to apply more strict restriction later

        return wordAttrList

    def getDataFrame(self):
        return self.__dataFrame

    def flattenJson(self):
        attrList = self.getAttrList()
        # pageAttrList, blockAttrList, lineAttrList = self.getAttrList()
        pageAttrList = attrList[0]
        blockAttrList = attrList[1]
        lineAttrList = attrList[2]

        blockAttrList = map(lambda x: list(["BlockList", x]), blockAttrList)
        lineAttrList = map(lambda x: list(["BlockList", "LineList", x]), lineAttrList)

        rawDF = json_normalize(self.getRawJson(), ["BlockList", "LineList", "WordList"],
                               pageAttrList + blockAttrList + lineAttrList)
        # rawDF.to_csv("C:/Temp/CorrectFlatten.csv", encoding="utf-8")

        # Rename the columns
        rawDF.rename(columns=lambda x: x.split(".")[-1], inplace=True)

        # Arrange the columns order
        colNames = ["PageID", "PageNo", "Lang", "XDPI", "YDPI", "PageStartX", "PageStartY", "PageWidth", "PageHeight", \
                    "PrintAreaStartX", "PrintAreaStartY", "PrintAreaWidth", "PrintAreaHeight", \
                    "BlockID", "BlockType", "BlockStartX", "BlockStartY", "BlockWidth", "BlockHeight", "BlockStyle", \
                    "LineID", "LineStartX", "LineStartY", "LineWidth", "LineHeight", "LineStyle", \
                    "WordID", "WordValue", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "Style font-name", "WordCN",
                    "CharList"]
        colNames = [colName for colName in colNames if colName in list(rawDF.columns.values)]
        rawDF = rawDF[colNames]

        # Change the column data type
        rawDF.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        self.__dataFrame = rawDF
        sd = self.__dataFrame['WordValue'],self.__dataFrame['WordID']
        print sd

    def export2csv(self, csvOutput):
        if self.getDataFrame().size == 0:
            self.flattenJson()

        self.getDataFrame().to_csv(csvOutput, encoding="utf-8")

if __name__=="__main__":
    jsoninput= '/Users/badrkhamis/Documents/DataScience/block_analysis/invoice.json'
    jsonClass = JsonDoc(jsoninput)
    jsonObject = jsonClass.flattenJson()
    jsonClass.export2csv('sds.csv')