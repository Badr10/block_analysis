"""
* Update History(Version 16):
*
* By Abisola Adeniran and Badr Khamis, for Cognitive Digitalization
*
* January 29th, 2016(Abisola Adeniran): Cleaned up code to remove unused scripts
*
* January 30th, 2016(Abisola Adeniran): Added IndexName replacements with Jena produced classnames
*
* January 30th, 2016(Badr Khamis): Added Semantic check using Jena Query By Ashraf Aswwad and produce confidence level
*
* Initial Modification History(Version 14 to 15):
*
* By Jianli Ren, for Cognitive Digitalization
*
* Oct 14th, 2016: Initial version, based on the Datacap XML - instead of the hOCR XHTML
*
* Oct 20th, 2016: Enhance the code to support multi-page, more specifically:
*                 (1) Document tag, which doesn't exist previously in the single page XML sample
*                 (2) Iterate the node attribute list to fix the assertion failure, as some attributes
*                     like printArea seems to be optional
*
* Oct 21st, 2016: Enhance the code to support dynamic schema
*                 (1) Replace the Alchemy API Key with the unlimited one provided by Elvir
*                 (2) Enhance the code to support dynamic schema
*
* Oct 25th, 2016: Add the Jena integration, i.e., query and update
*
* Oct 26th, 2016: Enrich the raw DataFrame by flattening JSON exported from XML, for standardization and info extraction
*
* Oct 27th, 2016: Enhance the script to support dynamic table schema
*
* Oct 31st, 2016: Change the output folder for JSON, as well as the Dataframe, WSTN, and INVAL file name
*
* Nov 04th, 2016: Enhance the script to fix issue of empty "pos" attribute, due to Datacap bug
*
* Nov 07th, 2016:  Multi-tab index-value pair
*
* Nov 09th, 2016: Add the __tableDataFrame to facilitate table cell text grouping
*
* Dec 06th, 2016: Clean-up the code, removed things like Alchemy & Jena integration, and added MongoDB support for X-Ray
*
"""

import sys, glob, os.path, datetime

from xml.dom.minidom import parse

import ijson
import json, re, copy

from SPARQLWrapper import SPARQLWrapper, JSON
from difflib import SequenceMatcher

import pandas as pd
from pandas.io.json import *
reload(sys)
sys.setdefaultencoding('utf-8')

######################### Step 1: Export the Datacap XML to JSON #########################
class XmlDoc:
    # Constants
    XML_TAG_DOCUMENT = "Document"
    XML_TAG_PAGE = "Page"
    XML_TAG_PICTURE = "Picture"
    XML_TAG_BLOCK = "Block"
    XML_TAG_TITLE = "Title"
    XML_TAG_TABLE = "Table"
    XML_TAG_PARA = "Para"
    XML_TAG_LINE = "L"
    XML_TAG_WORD = "W"
    XML_TAG_CHAR = "C"
    XML_TAG_STYLE = "Style"
    XML_TAG_ROW = "Row"
    XML_TAG_CELL = "Cell"

    XML_ATTRIBUTE_POS = "pos"
    XML_ATTRIBUTE_LANG = "lang"
    XML_ATTRIBUTE_ID = "id"
    XML_ATTRIBUTE_STYLE = "s"
    XML_ATTRIBUTE_PRINTAREA = "printArea"
    XML_ATTRIBUTE_XDPI = "xdpi"
    XML_ATTRIBUTE_YDPI = "ydpi"
    XML_ATTRIBUTE_CN = "cn"
    XML_ATTRIBUTE_VALUE = "v"

    XML_TABLE_ATTRIBUTE_ROWS = "rows"
    XML_TABLE_ATTRIBUTE_COLUMNS = "columns"
    XML_TABLE_ATTRIBUTE_ROWID = "row"
    XML_TABLE_ATTRIBUTE_COLUMNID = "col"
    XML_TABLE_ATTRIBUTE_ROWSPAN = "rowSpan"
    XML_TABLE_ATTRIBUTE_COLUMNSPAN = "columnSpan"
    PAGE_NODE = ""

    def __init__(self, xmlInput):
        # Read the raw Datacap XML file to get the XML/XHTML content
        with open(xmlInput, 'r') as xmlInputFile:
            self.xmlDOM = parse(xmlInputFile)

        xmlInputFile.close()


    def export2json(self, jsonOutput):
        # Changed to support the Document tag
        jsonDocument = self.extractInfo()

        with open(jsonOutput + ".json", 'w') as jsonFile:
            json.dump(jsonDocument, jsonFile)

        jsonFile.close()
        return jsonDocument


    def extractInfo(self):
        pageNodeList = [pgNode for pgNode in self.xmlDOM.getElementsByTagName(self.XML_TAG_PAGE)
                            if ((pgNode.nodeType == pgNode.ELEMENT_NODE) and (len(pgNode.childNodes) > 0))]

        pageList = []
        for pageNode in pageNodeList:
            page = {}
            # Page ID
            page["PageNo"] = pageNodeList.index(pageNode)

            # Page metadata/attributes begin
            # Page location & size
            # There might be a better way to see wether the attributes exist or not
            # Due to the lack of XML standard the if statement is added for ALL attributes to avoid failure
            if pageNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                page["PageStartX"], page["PageStartY"], page["PageWidth"], page["PageHeight"] = \
                    self.splitPos(pageNode.getAttribute(self.XML_ATTRIBUTE_POS))

            # Page language, ID, and style
            if pageNode.hasAttribute(self.XML_ATTRIBUTE_LANG):
                page["Lang"] = pageNode.getAttribute(self.XML_ATTRIBUTE_LANG)

            if pageNode.hasAttribute(self.XML_ATTRIBUTE_STYLE):
                page["PageStyle"] = pageNode.getAttribute(self.XML_ATTRIBUTE_STYLE)

            if pageNode.hasAttribute(self.XML_ATTRIBUTE_ID):
                page["PageID"] = pageNode.getAttribute(self.XML_ATTRIBUTE_ID)

            # Page print area location & size
            if pageNode.hasAttribute(self.XML_ATTRIBUTE_PRINTAREA):
                page["PrintAreaStartX"], page["PrintAreaStartY"], page["PrintAreaWidth"], page["PrintAreaHeight"] = \
                    self.splitPos(pageNode.getAttribute(self.XML_ATTRIBUTE_PRINTAREA))

            # Page DPI
            if pageNode.hasAttribute(self.XML_ATTRIBUTE_XDPI):
                page["XDPI"] = int(pageNode.getAttribute(self.XML_ATTRIBUTE_XDPI))

            if pageNode.hasAttribute(self.XML_ATTRIBUTE_YDPI):
                page["YDPI"] = int(pageNode.getAttribute(self.XML_ATTRIBUTE_YDPI))
            # Page metadata/attributes end

            # Page composite attributes, like Block, Title, Para, Table, etc.
            # Block, Title, Para have been standardized as "Block" even though the TAG_NAME is different
            self.PAGE_NODE = pageNode
            page["BlockList"] = self.extractBlock(pageNode)
            page["TableList"] = self.extractTable(pageNode)
            page["StyleList"] = self.extractStyle(pageNode)
            self.PAGE_NODE = pageNode

            pageList.append(page)

        # Document tag
        docNodeList = self.xmlDOM.getElementsByTagName(self.XML_TAG_DOCUMENT)
        # Due to the lack of XML standard, some XMLs have the "Document" tag some don't, thus comment out the assertion below
        # assert len(docNodeList) == 1

        jsonDoc = {}
        # Again, some XML has the "ID" attribute some don't
        if (len(docNodeList) > 0) and docNodeList[0].hasAttribute(self.XML_ATTRIBUTE_ID):
            jsonDoc["DocID"] = docNodeList[0].getAttribute(self.XML_ATTRIBUTE_ID)
        jsonDoc["Document"] = pageList

        return jsonDoc


    def splitPos(self, pos):
        # Split the "pos" attribute into four (4) separate attributes: StartX, StartY, Width, and Height
        # Yes we did experience the situation where just a bare "pos" attribute without any values
        if len(pos) == 0:
            return [0, 0, 0, 0]
        else:
            assert len(pos.split(",")) == 4
            return [int(pos.split(",")[0]), int(pos.split(",")[1]), \
                    int(pos.split(",")[2]) - int(pos.split(",")[0]), \
                    int(pos.split(",")[3]) - int(pos.split(",")[1])]

    def extractBlock(self, pageNode):
        blockNodeList = [bkNode for bkNode in pageNode.childNodes \
                            if ((bkNode.nodeName in [self.XML_TAG_BLOCK, self.XML_TAG_TITLE, self.XML_TAG_PARA]) and (len(bkNode.childNodes) > 0))]

        # Block
        blockList = []
        for blockNode in blockNodeList:
            block = {}
            # Block position & size
            block["BlockID"] = blockNodeList.index(blockNode)
            block["BlockType"] = blockNode.nodeName

            if blockNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                block["BlockStartX"], block["BlockStartY"], block["BlockWidth"], block["BlockHeight"] = \
                    self.splitPos(blockNode.getAttribute(self.XML_ATTRIBUTE_POS))

            # Block style
            if blockNode.hasAttribute(self.XML_ATTRIBUTE_STYLE):
                block["BlockStyle"] = blockNode.getAttribute(self.XML_ATTRIBUTE_STYLE)

            # Line
            block["LineList"] = self.extractLine(blockNode)
            blockList.append(block)

        return blockList

    def get_font(self, value):
        styleNodeList = [stlNode for stlNode in self.PAGE_NODE.childNodes if (stlNode.nodeName == self.XML_TAG_STYLE)]
        styleList = []
        for styleNode in styleNodeList:
            style = {}

            if styleNode.hasAttribute(self.XML_ATTRIBUTE_ID):
                style["StyleID"] = styleNode.getAttribute(self.XML_ATTRIBUTE_ID)

            if styleNode.hasAttribute(self.XML_ATTRIBUTE_VALUE):
                style["StyleValue"] = styleNode.getAttribute(self.XML_ATTRIBUTE_VALUE)
            styleList.append(style)

        new_value = ""
        for i in styleList:
            if i["StyleID"] == value:
                new_value = i["StyleValue"]
        return new_value

    def extract_font(self, font):
        color = None
        font_weight = None
        font_name = None
        font_family = None
        font_size = None
        splitted_font = font.split(';')
        for i in splitted_font:
            values = i.strip()
            if "font-name" in values:
                new_value = values.split(":")
                font_name = new_value[1]
            if "font-family" in values:
                new_value = values.split(":")
                font_family = new_value[1]
            if "font-size" in values:
                new_value = values.split(":")
                font_size = re.findall(r'\d+\.*\d*', new_value[1])
                # print font_size.pop()
            if "font-weight" in values:
                new_value = values.split(":")
                font_weight = new_value[1]

            if "color" in values:
                new_value = values.split(":")
                color = str(new_value[1])


        return color, font_family, font_name, font_size, font_weight


    def extractLine(self, blockNode):
        lineNodeList = [lnNode for lnNode in blockNode.childNodes if ((lnNode.nodeName == self.XML_TAG_LINE) and (len(lnNode.childNodes) > 0))]


        lineList = []
        for lineNode in lineNodeList:
            line = {}

            line["LineID"] = lineNodeList.index(lineNode)
            if lineNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                line["LineStartX"], line["LineStartY"], line["LineWidth"], line["LineHeight"] = \
                    self.splitPos(lineNode.getAttribute(self.XML_ATTRIBUTE_POS))
            # Line style (?)
            if lineNode.hasAttribute(self.XML_ATTRIBUTE_STYLE):
                line["LineStyle"] = lineNode.getAttribute(self.XML_ATTRIBUTE_STYLE)

            # Word
            wordList = []
            wordNodeList = [wdNode for wdNode in lineNode.childNodes if ((wdNode.nodeName == self.XML_TAG_WORD) and (len(wdNode.childNodes) > 0))]

            for wordNode in wordNodeList:
                word = {}
                word["WordID"] = wordNodeList.index(wordNode)

                if wordNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                    word["WordStartX"], word["WordStartY"], word["WordWidth"], word["WordHeight"] = \
                        self.splitPos(wordNode.getAttribute(self.XML_ATTRIBUTE_POS))
                # Word style
                if wordNode.hasAttribute(self.XML_ATTRIBUTE_VALUE):
                    word["WordValue"] = wordNode.getAttribute(self.XML_ATTRIBUTE_VALUE)

                if wordNode.hasAttribute(self.XML_ATTRIBUTE_STYLE):
                    font_id = wordNode.getAttribute(self.XML_ATTRIBUTE_STYLE)
                    font_style = self.get_font(font_id)
                    color, font_family, font_name, font_size, font_weight = self.extract_font(font_style)
                    word["FontColor"] = str(color)
                    word["FontFamily"] = font_family
                    word["FontSize"] = font_size.pop()
                    word["FontWeight"] = font_weight
                    word["FontName"] = font_name
                    word["WordStyle"] = font_id


                if wordNode.hasAttribute(self.XML_ATTRIBUTE_CN):
                    word["WordCN"] = wordNode.getAttribute(self.XML_ATTRIBUTE_CN)

                # Char
                charList = []
                charNodeList = [chNode for chNode in wordNode.childNodes if (chNode.nodeName == self.XML_TAG_CHAR)]

                for charNode in charNodeList:
                    char = {}
                    char["CharID"] = charNodeList.index(charNode)

                    if charNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                        char["CharStartX"], char["CharStartY"], char["CharWidth"], char["CharHeight"] = \
                            self.splitPos(charNode.getAttribute(self.XML_ATTRIBUTE_POS))

                    # Char style
                    if charNode.hasAttribute(self.XML_ATTRIBUTE_VALUE):
                        char["CharValue"] = charNode.getAttribute(self.XML_ATTRIBUTE_VALUE)

                    if charNode.hasAttribute(self.XML_ATTRIBUTE_STYLE):
                        char["CharStyle"] = charNode.getAttribute(self.XML_ATTRIBUTE_STYLE)

                    if charNode.hasAttribute(self.XML_ATTRIBUTE_CN):
                        char["CharCN"] = charNode.getAttribute(self.XML_ATTRIBUTE_CN)

                    charList.append(char)

                word["CharList"] = charList
                wordList.append(word)

            line["WordList"] = wordList
            lineList.append(line)

        return lineList


    def extractPic(self, pageNode):
        picNodeList = [picNode for picNode in pageNode.childNodes if (picNode.nodeName == self.XML_TAG_PICTURE)]

        picList = []
        for picNode in picNodeList:
            pic = {}
            pic["PicID"] = picNodeList.index(picNode)

            if picNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                pic["PicStartX"], pic["PicStartY"], pic["PicWidth"], pic["PicHeight"] = \
                    self.splitPos(picNode.getAttribute(self.XML_ATTRIBUTE_POS))

            picList.append(pic)

        return picList


    def extractTable(self, pageNode):
        tableNodeList = [tblNode for tblNode in pageNode.childNodes if ((tblNode.nodeName == self.XML_TAG_TABLE) and (len(tblNode.childNodes) > 0))]

        # Table
        # For table, the hierarchy is: Table -> RowList -> Row -> CellList -> Cell -> Line
        # So here we do the parsing of Table -> RowList -> Row -> CellList -> Cell and reuse the extractLine,
        # which is shared across Block, Paragrah, Title, etc.
        tableList = []
        for tableNode in tableNodeList:
            table = {}
            table["TableID"] = tableNodeList.index(tableNode)

            if tableNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                table["TableStartX"], table["TableStartY"], table["TableWidth"], table["TableHeight"] = \
                    self.splitPos(tableNode.getAttribute(self.XML_ATTRIBUTE_POS))

            if tableNode.hasAttribute(self.XML_TABLE_ATTRIBUTE_ROWS):
                table["TableRows"] = tableNode.getAttribute(self.XML_TABLE_ATTRIBUTE_ROWS)

            if tableNode.hasAttribute(self.XML_TABLE_ATTRIBUTE_COLUMNS):
                table["TableCols"] = tableNode.getAttribute(self.XML_TABLE_ATTRIBUTE_COLUMNS)

            if tableNode.hasAttribute(self.XML_ATTRIBUTE_STYLE):
                table["TableStyle"] = tableNode.getAttribute(self.XML_ATTRIBUTE_STYLE)

            # Row
            rowList = []
            rowNodeList = [rwNode for rwNode in tableNode.childNodes if ((rwNode.nodeName == self.XML_TAG_ROW) and (len(rwNode.childNodes) > 0))]

            for rowNode in rowNodeList:
                row = {}
                row["RowID"] = rowNodeList.index(rowNode)

                if tableNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                    row["RowStartX"], row["RowStartY"], row["RowWidth"], row["RowHeight"] = \
                        self.splitPos(rowNode.getAttribute(self.XML_ATTRIBUTE_POS))

                if tableNode.hasAttribute(self.XML_ATTRIBUTE_STYLE):
                    row["RowStyle"] = rowNode.getAttribute(self.XML_ATTRIBUTE_STYLE)

                # Cell
                cellList = []
                cellNodeList = [clNode for clNode in rowNode.childNodes if ((clNode.nodeName == self.XML_TAG_CELL) and (len(clNode.childNodes) > 0))]

                for cellNode in cellNodeList:
                    cell = {}
                    cell["CellID"] = cellNodeList.index(cellNode)

                    if cellNode.hasAttribute(self.XML_ATTRIBUTE_POS):
                        cell["CellStartX"], cell["CellStartY"], cell["CellWidth"], cell["CellHeight"] = \
                            self.splitPos(cellNode.getAttribute(self.XML_ATTRIBUTE_POS))

                    if cellNode.hasAttribute(self.XML_TABLE_ATTRIBUTE_ROWID):
                        cell["CellRowID"] = cellNode.getAttribute(self.XML_TABLE_ATTRIBUTE_ROWID)

                    if cellNode.hasAttribute(self.XML_TABLE_ATTRIBUTE_COLUMNID):
                        cell["CellColumnID"] = cellNode.getAttribute(self.XML_TABLE_ATTRIBUTE_COLUMNID)

                    if cellNode.hasAttribute(self.XML_TABLE_ATTRIBUTE_ROWSPAN):
                        cell["CellRowSpan"] = cellNode.getAttribute(self.XML_TABLE_ATTRIBUTE_ROWSPAN)

                    if cellNode.hasAttribute(self.XML_TABLE_ATTRIBUTE_COLUMNSPAN):
                        cell["CellColumnSpan"] = cellNode.getAttribute(self.XML_TABLE_ATTRIBUTE_COLUMNSPAN)

                    # Line
                    cell["LineList"] = self.extractLine(cellNode)

                    cellList.append(cell)

                row["CellList"] = cellList
                rowList.append(row)

            table["RowList"] = rowList
            tableList.append(table)

        return tableList

    def extractStyle(self, pageNode):
        styleNodeList = [stlNode for stlNode in pageNode.childNodes if (stlNode.nodeName == self.XML_TAG_STYLE)]

        # Style
        styleList = []
        for styleNode in styleNodeList:
            style = {}

            if styleNode.hasAttribute(self.XML_ATTRIBUTE_ID):
                style["StyleID"] = styleNode.getAttribute(self.XML_ATTRIBUTE_ID)

            if styleNode.hasAttribute(self.XML_ATTRIBUTE_VALUE):
                styleValue = styleNode.getAttribute(self.XML_ATTRIBUTE_VALUE).split(";")

            for index in range(len(styleValue)-1):
                if len(styleValue[index]) > 3:
                    style["Style" + styleValue[index].split(":")[0]] = styleValue[index].split(":")[1]

            styleList.append(style)

        return styleList


######################### Step 2: Flatten JSON to extract info #########################
class JsonDoc:
    # Global constants/attribute names
    PAGE_ATTR_LIST = ["PageNo", "PageID", "Lang", "XDPI", "YDPI", "PageStartX", "PageStartY", "PageWidth", "PageHeight", \
                      "PrintAreaStartX", "PrintAreaStartY", "PrintAreaWidth", "PrintAreaHeight"]
    BLOCK_ATTR_LIST = ["BlockID", "BlockType", "BlockStartX", "BlockStartY", "BlockWidth", "BlockHeight", "BlockStyle"]
    LINE_ATTR_LIST = ["LineID", "LineStartX", "LineStartY", "LineWidth", "LineHeight", "LineStyle"]

    TABLE_ATTR_LIST = ["TableID", "TableStartX", "TableStartY", "TableWidth", "TableHeight", "TableRows", "TableCols", "TableStyle"]
    ROW_ATTR_LIST = ["RowID", "RowStartX", "RowStartY", "RowWidth", "RowHeight", "RowStyle"]
    CELL_ATTR_LIST = ["CellID", "CellRowID", "CellColumnID", "CellStartX", "CellStartY", "CellWidth", "CellHeight", "CellRowSpan", "CellColumnSpan"]

    WORD_ATTR_LIST = ["WordID", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordStyle", "FontColor", "FontSize", "FontFamily", "FontWeight", "FontName"]


    def __init__(self, jsonInput):
        # Read json file as input
        blocks = []
        with open(jsonInput, 'r') as jsonInputFile:
            try:
                objects = ijson.items(jsonInputFile, "Document.item")
                blocks = list(objects)
            except:
                pass

        jsonInputFile.close()

        self.__rawJson = blocks
        self.__dataFrame = pd.DataFrame()
        self.__tableDataFrame = pd.DataFrame()


    def getRawJson(self):
        return self.__rawJson

    def getTableDataFrame(self):
        return self.__tableDataFrame

    def getAttrList(self):
        """
        This function returns the list of attributes for Page, Block, and Line,
        as some attributes are optional so for JSON flattening we need to dynamically
        determine the list of attributes so we can construct the Pandas DataFrame accordingly.
        """
        pageAttrList = self.PAGE_ATTR_LIST
        blockAttrList = self.BLOCK_ATTR_LIST
        lineAttrList = self.LINE_ATTR_LIST

        # Page attributes
        for page in self.getRawJson():
            pageAttrList = list(set(pageAttrList).intersection(set(page.keys())))

            # Block attributes
            for block in page["BlockList"]:
                blockAttrList = list(set(blockAttrList).intersection(set(block.keys())))

                # Line attributes
                for line in block["LineList"]:
                    lineAttrList = list(set(lineAttrList).intersection(set(line.keys())))

        return pageAttrList, blockAttrList, lineAttrList


    def getWordAttrList(self):
        """
        The same as the getAttrList above for Page, Block, and Line, but seems the Word attribute list
        is pretty stable/consistent so just using the Word WORD_ATTR_LIST constant, for now.
        """
        wordAttrList = self.WORD_ATTR_LIST

        # Return the original word attribute list for now, maybe need to apply more strict restriction later
        return wordAttrList

    def flattenJson(self):
        attrList = self.getAttrList()

        pageAttrList = attrList[0]
        blockAttrList = attrList[1]
        lineAttrList = attrList[2]

        # Construct the Block and Line attribute names hierarchy, for JSON flattening, using json_normalize
        blockAttrList = map(lambda x: list(["BlockList", x]), blockAttrList)
        lineAttrList = map(lambda x: list(["BlockList", "LineList", x]), lineAttrList)

        rawDF = json_normalize(self.getRawJson(), ["BlockList", "LineList", "WordList"], pageAttrList + blockAttrList + lineAttrList)

        # Rename the columns, keep only the last section, from the attribute names hierarchy
        rawDF.rename(columns=lambda x: x.split(".")[-1], inplace=True)

        # Arrange the columns order
        colNames = ["PageID", "PageNo", "Lang", "XDPI", "YDPI", "PageStartX", "PageStartY", "PageWidth", "PageHeight", \
                    "PrintAreaStartX", "PrintAreaStartY", "PrintAreaWidth", "PrintAreaHeight", \
                    "BlockID", "BlockType", "BlockStartX", "BlockStartY", "BlockWidth", "BlockHeight", "BlockStyle", \
                    "LineID", "LineStartX", "LineStartY", "LineWidth", "LineHeight", "LineStyle", \
                    "WordID", "WordValue", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordStyle", "FontColor", "FontSize", "FontFamily", "FontWeight", "FontName", "WordCN", "CharList"]
        colNames = [colName for colName in colNames if colName in list(rawDF.columns.values)]
        rawDF = rawDF[colNames]

        # Change the column data type
        rawDF.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        self.__dataFrame = rawDF

    def getDataFrame(self):
        return self.__dataFrame

    def getTextByLine(self):
        lineTextDF = (self.getDataFrame().groupby(["PageNo", "BlockID", "BlockType", "LineID"], sort=True, as_index=False)
                      .agg({"WordValue": lambda x: " ".join(x).strip(), "WordID": lambda x: x.nunique(), "WordStyle": lambda x: x.nunique()})
                      .rename(columns = {"WordValue": "LineText", "WordID": "LineWordCount", "WordStyle": "WordStyleCount"}))

        return lineTextDF

    def getTextByBlock(self):
        blockTextDF = (self.getDataFrame().groupby(["PageNo", "BlockID"], sort=True, as_index=False)
                       .agg({"WordValue": lambda x: " ".join(x).strip(), "LineID": lambda x: x.nunique(), "WordID": lambda x: x.count()})
                       .rename(columns = {"WordValue": "BlockText", "LineID": "LineCount", "WordID": "BlockWordCount"}))
        blockTextDF["AvgLineDensity"] = (blockTextDF["BlockWordCount"] / blockTextDF["LineCount"]).astype(int)

        return blockTextDF


    def getTableAttrList(self):
        # Same idea as the getAttrList above, just for Table
        tableAttrList = self.TABLE_ATTR_LIST
        rowAttrList = self.ROW_ATTR_LIST
        cellAttrList = self.CELL_ATTR_LIST
        lineAttrList = self.LINE_ATTR_LIST

        # Page attributes
        for page in self.getRawJson():
            # Table attributes
            for table in [tbl for tbl in page["TableList"] if len(tbl) > 0]:
                tableAttrList = list(set(tableAttrList).intersection(set(table.keys())))

                # Row attributes
                for row in table["RowList"]:
                    rowAttrList = list(set(rowAttrList).intersection(set(row.keys())))

                    # Cell attributes
                    for cell in row["CellList"]:
                        cellAttrList = list(set(cellAttrList).intersection(set(cell.keys())))

                        # Line attributes
                        for line in cell["LineList"]:
                            lineAttrList = list(set(lineAttrList).intersection(set(line.keys())))

        return tableAttrList, rowAttrList, cellAttrList, lineAttrList


    def getTableText(self):
        # In case there is no Table in the XML document
        if len(self.getRawJson()[0]["TableList"]) == 0:
            return pd.DataFrame()
        else:
            tableAttrList = self.getTableAttrList()[0]
            rowAttrList = self.getTableAttrList()[1]
            cellAttrList = self.getTableAttrList()[2]
            lineAttrList = self.getTableAttrList()[3]

            # Construct the attribute names for JSON flattening, using json_normalize()
            tableAttrList = map(lambda x: list(["TableList", x]), tableAttrList)
            rowAttrList = map(lambda x: list(["TableList", "RowList", x]), rowAttrList)
            cellAttrList = map(lambda x: list(["TableList", "RowList", "CellList", x]), cellAttrList)
            lineAttrList = map(lambda x: list(["TableList", "RowList", "CellList", "LineList", x]), lineAttrList)

            # Flatten the JSON using the json_normalize() function
            rawDF = json_normalize(self.getRawJson(), ["TableList", "RowList", "CellList", "LineList", "WordList"], tableAttrList + rowAttrList + cellAttrList + lineAttrList)

            # Rename the columns, keep only the last section of the attribute name hierarchy
            rawDF.rename(columns=lambda x: x.split(".")[-1], inplace=True)

            # Arrange the columns order
            colNames = ["TableID", "TableStartX", "TableStartY", "TableWidth", "TableHeight", "TableRows", "TableCols", "TableStyle", \
                        "RowID", "RowStartX", "RowStartY", "RowWidth", "RowHeight", "RowStyle", \
                        "CellID", "CellRowID", "CellColumnID", "CellStartX", "CellStartY", "CellWidth", "CellHeight", "LineStyle", \
                        "LineID", "LineStartX", "LineStartY", "LineWidth", "LineHeight", "LineStyle", \
                        "WordID", "WordValue", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordStyle", "FontColor", "FontSize", "FontFamily", "FontWeight", "FontName", "WordCN", "CharList"]
            colNames = [colName for colName in colNames if colName in list(rawDF.columns.values)]
            rawDF = rawDF[colNames]

            # Change the column data type
            rawDF.apply(lambda x: pd.to_numeric(x, errors='ignore'))

            # With no Table in the original XML document
            if len(self.__tableDataFrame) == 0:
                self.__tableDataFrame = rawDF

            return (rawDF.groupby(["TableID", "RowID", "CellColumnID"], sort=True, as_index=False)
                    .agg({"WordValue": lambda x: " ".join(x).strip()}))


    def enrichTableDataFrame(self):
        lineTextDF = self.getTableTextByLine()
        rawTableDataFrame = self.getTableDataFrame()

        enrichedTableDataFrame = pd.merge(rawTableDataFrame, lineTextDF, on=["TableID", "RowID", "CellColumnID", "LineID"], how="left", suffixes=("_Raw", "_Line"))
        enrichedTableDataFrame.rename(columns = {"WordValue_Raw": "WordValue", "WordValue_Line": "LineText"}, inplace=True)

        """
        The code fragement below separate lines based on distance, using Pandas dataframe aggregate function

        First sort the dataframe by ["TableID", "RowID", "CellColumnID", "LineID", "WordID"], ascendingly
        Then calcuate the distance/gap between the current word and its successor, and fillna with 0
        Merge with the original dataframe, and then based on the distance/gap to flag those word(s) split the line to "TextGroup"

        Note the 1.5, indicats the distance between words flags the boundary of "TextGroup", i.e., multi-tab index-value pair.
        The magic number might be changed, it's hard coded here during POC.
        """
        enrichedTableDataFrame.sort_values(["TableID", "RowID", "CellColumnID", "LineID", "WordID"], ascending=[True, True, True, True, True])
        enrichedTableDataFrame["WordDistance"] = (enrichedTableDataFrame.groupby(["TableID", "RowID", "CellColumnID", "LineID"])
                                                  .apply(lambda x: x["WordStartX"].diff() - x["WordWidth"].shift())
                                                  .fillna(0).values)
        enrichedTableDataFrame["WordDistance"].fillna(0)
        tempDF = pd.DataFrame()
        tempDF = (enrichedTableDataFrame.groupby(["TableID", "RowID", "CellColumnID", "LineID"], as_index=False)
                  .mean()
                  .rename(columns = {"WordDistance": "AvgWordDistance"}))
        tempDF = tempDF[["TableID", "RowID", "CellColumnID", "LineID", "AvgWordDistance"]]

        # Join back with the original dataframe
        enrichedTableDataFrame = pd.merge(enrichedTableDataFrame, tempDF, on=["TableID", "RowID", "CellColumnID", "LineID"], how="left")

        enrichedTableDataFrame["TextGroupSeparator"] = enrichedTableDataFrame["WordDistance"] >= enrichedTableDataFrame["AvgWordDistance"] * 1.5


        return enrichedTableDataFrame


    def getTableTextByLine(self):
        if len(self.getRawJson()[0]["TableList"]) == 0:
            return pd.DataFrame()
        else:
            tableAttrList = self.getTableAttrList()[0]
            rowAttrList = self.getTableAttrList()[1]
            cellAttrList = self.getTableAttrList()[2]
            lineAttrList = self.getTableAttrList()[3]

            tableAttrList = map(lambda x: list(["TableList", x]), tableAttrList)
            rowAttrList = map(lambda x: list(["TableList", "RowList", x]), rowAttrList)
            cellAttrList = map(lambda x: list(["TableList", "RowList", "CellList", x]), cellAttrList)
            lineAttrList = map(lambda x: list(["TableList", "RowList", "CellList", "LineList", x]), lineAttrList)

            rawDF = json_normalize(self.getRawJson(), ["TableList", "RowList", "CellList", "LineList", "WordList"], tableAttrList + rowAttrList + cellAttrList + lineAttrList)

            # Rename the columns
            rawDF.rename(columns=lambda x: x.split(".")[-1], inplace=True)

            # Arrange the columns order
            colNames = ["TableID", "TableStartX", "TableStartY", "TableWidth", "TableHeight", "TableRows", "TableCols", "TableStyle", \
                        "RowID", "RowStartX", "RowStartY", "RowWidth", "RowHeight", "RowStyle", \
                        "CellID", "CellRowID", "CellColumnID", "CellStartX", "CellStartY", "CellWidth", "CellHeight", "LineStyle", \
                        "LineID", "LineStartX", "LineStartY", "LineWidth", "LineHeight", "LineStyle", \
                        "WordID", "WordValue", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordStyle", "FontColor", "FontSize", "FontFamily", "FontWeight", "FontName", "WordCN", "CharList"]
            colNames = [colName for colName in colNames if colName in list(rawDF.columns.values)]
            rawDF = rawDF[colNames]

            # Change the column data type
            rawDF.apply(lambda x: pd.to_numeric(x, errors='ignore'))
            if len(self.__tableDataFrame) == 0:
                self.__tableDataFrame = rawDF

            return (rawDF.groupby(["TableID", "RowID", "CellColumnID", "LineID"], sort=True, as_index=False)
                    .agg({"WordValue": lambda x: " ".join(x).strip()}))


    def enrichDataFrame(self, enrichedOutput):
        blockTextDF = self.getTextByBlock()
        lineTextDF = self.getTextByLine()

        enrichedDataFrame = pd.merge(self.getDataFrame(), blockTextDF, on=["PageNo", "BlockID"], how="left", suffixes=("_Raw", "_Block"))

        enrichedDataFrame = pd.merge(enrichedDataFrame, lineTextDF, on=["PageNo", "BlockID", "LineID"], how="left", suffixes=("_Raw", "_Line"))
        enrichedDataFrame.drop("BlockType_Line", axis=1, inplace=True)

        enrichedDataFrame.rename(columns = {"BlockType_Raw": "BlockType"}, inplace=True)

        # Separate lines based on distance, same idea as the enrichTableDataFrame() function above
        enrichedDataFrame.sort_values(["PageNo", "BlockID", "LineID", "WordID"], ascending=[True, True, True, True])
        enrichedDataFrame["WordDistance"] = (enrichedDataFrame.groupby(["PageNo", "BlockID", "LineID"])
                                             .apply(lambda x: x["WordStartX"].diff() - x["WordWidth"].shift())
                                             .fillna(0).values)

        tempDF = pd.DataFrame()
        tempDF = (enrichedDataFrame.groupby(["PageNo", "BlockID", "LineID"], as_index=False)
                  .mean()
                  .rename(columns = {"WordDistance": "AvgWordDistance"}))
        tempDF = tempDF[["PageNo", "BlockID", "LineID", "AvgWordDistance"]]
        enrichedDataFrame = pd.merge(enrichedDataFrame, tempDF, on=["PageNo", "BlockID", "LineID"], how="left")

        enrichedDataFrame["TextGroupSeparator"] = enrichedDataFrame["WordDistance"] >= enrichedDataFrame["AvgWordDistance"] * 2

        enrichedDataFrame.to_csv(enrichedOutput, encoding="utf-8")

        return enrichedDataFrame
        # print enrichedDataFrame



class ProcessKeyValue:

    def __init__(self, json_input, output_dir, file_name):
        self.json_input = json_input
        self.output_dir = output_dir
        self.file_name = file_name
        self.jsonObject = JsonDoc(self.json_input)
        self.jsonObject.flattenJson()
        self.enrichedDataFrame = self.jsonObject.enrichDataFrame(self.output_dir + "/RICH-" + self.file_name + ".csv")
        self.TableDataFrame = self.jsonObject.enrichTableDataFrame()
    def process_input(self):
        # jsonObject = JsonDoc(self.json_input)
        # print self.TableDataFrame.head(6)

        # print jsonObject.DataFrame_analytic()
        # Enriched data frame in csv, including:
        # Block: BlockText, BlockWordCount, LineCount, AvgLineDensity,
        # Line: LineText, LineWordCount, WordStyleCount
        # Word: WordDistance, AvgWordDistance, TextGroupSeparator
        # enrichedDataFrame = self.jsonObject.enrichDataFrame(self.output_dir + "/RICH-" + self.file_name + ".csv")


        ######################### Index-Value Pair parsing ...
        # Block component
        # Pre-processing: remove those unnecessary blocks and/or lines, e.g., close to header/footer, too short/thin ...

        # All the "sparse" blocks
        sparseBlock = self.enrichedDataFrame[self.enrichedDataFrame["AvgLineDensity"] <= 10]
        print sparseBlock
        # Remove blocks close enough to header
        middleBlock = sparseBlock[
            sparseBlock["BlockStartY"] + sparseBlock["BlockHeight"] - sparseBlock["PageStartY"] >= 100]

        # Remove blocks close enough to footer
        middleBlock = sparseBlock[sparseBlock["PageStartY"] + sparseBlock["PageHeight"] \
                                  - sparseBlock["BlockStartY"] - sparseBlock["BlockHeight"] >= 100]


        # Split
        indexValueList = []

        kvPairBlock = middleBlock[["PageNo", "BlockID", "LineID", "LineText", "WordID", "TextGroupSeparator"]]
        kvPairBlock = kvPairBlock[kvPairBlock["TextGroupSeparator"] == True]
        sepPosition = pd.Series()
        for text in kvPairBlock["LineText"].unique():
            sepPosition = kvPairBlock.loc[kvPairBlock["LineText"] == text, "WordID"]
            assert len(sepPosition.values) > 0
            indexValueList.extend(self.extractIndexValuePair(text, sepPosition))

        # Table component
            self.jsonObject.getTableTextByLine()
        if len(self.jsonObject.getTableDataFrame()) > 0:
            enrichedTableDataFrame = self.jsonObject.enrichTableDataFrame()

            for text in enrichedTableDataFrame["LineText"].unique():
                sepPosition = enrichedTableDataFrame.loc[(enrichedTableDataFrame["LineText"] == text) & (
                    enrichedTableDataFrame["TextGroupSeparator"] == True), "WordID"]

                if len(sepPosition.values) > 0:
                    indexValueList.extend(self.extractIndexValuePair(text, sepPosition))

        alias_index_list = self._process_JSON(indexValueList)
        self._export2Json(self.output_dir, self.file_name, alias_index_list)

    # Badr Khamis
    def DataFrame_analytic(self):
        # new_dataframe =  self.enrichedDataFrame[['WordID',"WordValue", "WordStartX", "WordStartY", "WordWidth", "WordHeight", "FontSize", "FontFamily", "FontWeight","WordDistance","TextGroupSeparator"]]
        Block_dataframe = self.enrichedDataFrame[
            ['WordID', "WordValue","FontColor", "FontSize", "FontFamily", "FontName",
             "FontWeight","WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordDistance","LineID","LineText","LineStartX", "LineStartY", "LineWidth", "LineHeight","AvgLineDensity","TextGroupSeparator"]]
        Table_dataframe = self.TableDataFrame[
            ['WordID', "WordValue","FontColor", "FontSize", "FontFamily", "FontName",
             "FontWeight","WordStartX", "WordStartY", "WordWidth", "WordHeight", "WordDistance","LineID","LineText","LineStartX", "LineStartY", "LineWidth", "LineHeight","TextGroupSeparator"]]
        print Table_dataframe
        df_new = pd.concat([Block_dataframe, Table_dataframe])
        print df_new
        # middle_blocks = df_new[df_new["AvgLineDensity"] <= 10]
        df_new.to_csv('tax.csv')
        # print middle_blocks.head(10)


    def extractIndexValuePair(self, lineText, sepLocationSeries):
        indexValueDict = {}
        indexValueList = []

        # Single index-value pair, separated with ":"
        if lineText.count(":") == 1:
            lineSplit = lineText.split(":")
            indexValueDict["IndexName"] = lineSplit[0].strip()
            indexValueDict["IndexValue"] =  lineSplit[1].strip()
            indexValueList.append(indexValueDict.copy())
        else:
            # Multi-tab index-value pairs scenario
            assert len(sepLocationSeries.values) > 0

            beginTextGroup = lineText.split()[:sepLocationSeries.values[0]]
            endTextGroup = lineText.split()[sepLocationSeries.values[len(sepLocationSeries.values) - 1]:]
            textGroupList = []
            if len(sepLocationSeries.values) > 1:
                for i in range(len(sepLocationSeries.values) - 1):
                    textGroupList.append(lineText.split()[sepLocationSeries.values[i]:sepLocationSeries.values[i+1]])

            textGroupList.insert(0, beginTextGroup)
            textGroupList.insert(len(textGroupList), endTextGroup)

            for textGroup in [" ".join(txtGrp) for txtGrp in textGroupList]:
                if textGroup.count(":") == 1:
                    splittedGroup = textGroup.split(":")
                    try:
                        indexValueDict["IndexName"] =  splittedGroup[0].strip()
                        indexValueDict["IndexValue"] =  splittedGroup[1].strip()
                        indexValueList.append(indexValueDict.copy())
                    except:
                        pass
                elif textGroup.count(":") > 1:
                    for kvPair in textGroup.split():
                        if not "ODP" in kvPair:
                            splittedKV = kvPair.split(":")
                            try:
                                indexValueDict["IndexName"] =  splittedKV[0].strip()
                                indexValueDict["IndexValue"] =  splittedKV[1].strip()
                                indexValueList.append(indexValueDict.copy())
                            except:
                                pass
        return indexValueList

    """Begin Abisola Adeniran"""

    def _remove_extra_space(self, first_value, second_value):
        # remove extra spaces
        first_value = re.sub('\s+', ' ', first_value)
        second_value = re.sub('\s+', ' ', second_value)
        return first_value, second_value

    def _compare_without_chars(self, first_value, second_value):
        # compile all non-space and non-word characters
        regex = re.compile(r'[^\s\w]')
        # replace all non-space and non-word characters in strings and compare
        return regex.sub('', first_value), regex.sub('', second_value)

    def _process_JSON(self, current_index_list):
        processed_details = []
        index_value_list = []
        # loop through the index value list
        for current_index in current_index_list:
            new_detail = copy.copy(current_index)
            # get semantic match, confidence and class name from function
            SemanticMatch, Confidence, ClassName = self.processSemantic(new_detail["IndexName"])
            # replace indexname if confidence is more than or equal to 70%
            if int(Confidence) >= 70:
                new_detail['SemanticMatch'] = SemanticMatch
                new_detail['Confidence'] = Confidence
                new_detail['IndexName'] = ClassName
                # check to ensure the particular index has not been processed
                if current_index not in processed_details:
                    processed_details.append(current_index)
                    index_value_list.append(new_detail)

        # retrieve the dictionaries that couldn't be processed
        remaining_details = [current_dict for current_dict in current_index_list if
                             current_dict not in processed_details]
        # combine the processed and unprocessed list
        index_value_list.extend(remaining_details)

        #get a final list with cleaned data
        final_index_value_list = []
        #check the unprocessed dictionaries and add new key values if they don't exist
        for index in index_value_list:
            if 'SemanticMatch' not in index:
                index['SemanticMatch'] = None
                index['Confidence'] = 0
                final_index_value_list.append(index)
            else:
                final_index_value_list.append(index)

        return final_index_value_list

    # loop though bind list to process confidence level
    def _process_bind_list(self, bindings_list, index_name):
        bind_list = []
        for binding in bindings_list:
            # create a dictionary that contains the label name and class name
            new_bind = {}
            new_bind["Class"] = binding["Property"]["value"]
            new_bind["Label"] = binding["Label"]["value"]
            bind_list.append(new_bind)

        #loop through the new dictionary with label name and use sequence matcher to determine the ratio
        new_bind_list = []
        for bind in bind_list:
            value = bind["Label"].lower().strip()
            index_name, value = self._compare_without_chars(index_name, value)
            index_name, value = self._remove_extra_space(index_name, value)
            ratio = SequenceMatcher(None, value, index_name).ratio()
            # add new key values to the dictionary only if the ratio gotten is more than 0.7
            if ratio >= 0.7:
                new_dict = {}
                new_dict['Confidence'] = (ratio / 1.0) * 100
                new_dict['SemanticMatch'] = bind["Label"]
                new_dict['ClassName'] = bind["Class"]
                new_bind_list.append(new_dict)
            else:
                new_dict = {}
                new_dict['Confidence'] = (ratio / 1.0) * 100
                new_dict['SemanticMatch'] = bind["Label"]
                new_dict['ClassName'] = bind["Class"]
                new_bind_list.append(new_dict)
        #get the dictionary with the highest confidence and return results
        max_bind_list = max(new_bind_list, key=lambda x: x['Confidence'])
        SemanticMatch = max_bind_list['SemanticMatch']
        Confidence = max_bind_list['Confidence']
        ClassName = max_bind_list['ClassName']

        return SemanticMatch, Confidence, ClassName

    """End Abisola Adeniran"""

    """Begin Badr Khamis"""
    def runJenaQuery(self, query):
        # queryEndPoint = "http://169.53.162.228:3030/finc/query"
        queryEndPoint = "http://169.53.162.228:3030/document/query"

        sparql = SPARQLWrapper(queryEndPoint)

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        return sparql.query().convert()

    def processSemantic(self, index_name):
        queryTemplate = ("""
                   prefix owl: <http://www.w3.org/2002/07/owl#>
                    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    prefix inv: <http://cognitivedigitization.mybluemix.net#>
                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                    SELECT ?Property ?Label  ?Class
                    WHERE {
                      ?subject rdfs:domain ?class .
                      bind(strafter(str(?subject),str(inv:)) AS ?Property) .
                      bind(strafter(str(?class),str(inv:)) AS ?Class) .
                      ?subject a owl:DatatypeProperty ;
                      rdfs:label ?Label ;
                      filter regex(?Label, "Para2Replace", "i")
                     }
            """)
        indexName = index_name.lower().strip()
        updatedQuery = queryTemplate.replace("Para2Replace", indexName)

        results = self.runJenaQuery(updatedQuery)
        SemanticMatch = None
        Confidence = 0
        ClassName = ""
        if len(results["results"]["bindings"]) == 0:
            SemanticMatch = None
            Confidence = str(0)
        else:
            bindingsList = results["results"]["bindings"]
            SemanticMatch, Confidence, ClassName = self._process_bind_list(bindingsList, indexName)
        return SemanticMatch, Confidence, ClassName

    """End Badr Khamis"""

    def _export2Json(self, outputDir, fileName, jsonOutput):
        with open(outputDir + "/INVAL-" + fileName + ".json", 'w') as ivmJsonFile:
            json.dump(jsonOutput, ivmJsonFile)
        ivmJsonFile.close()


if __name__ == "__main__":
    # get initial parameter - the path to JSON file
    path_details = sys.argv[1]

    # split the path to get the directory path without the file name
    output_dir = os.path.split(path_details)[0]
    # get the complete path passed
    xmlInputDir = path_details

    for xmlInput in glob.glob(xmlInputDir):
        xmlObject = XmlDoc(xmlInput)
        xmlObject.export2json(output_dir + "/" + os.path.split(xmlInput)[1])
        # xmlObject.export2json(os.path.split(sys.argv[1])[0] + "/" + os.path.split(xmlInput)[1])

        json_input = output_dir + "/" + os.path.split(xmlInput)[1] + ".json"
        # get the file name
        file_name = os.path.split(xmlInput)[1]
        # process the key value
        process_key_value = ProcessKeyValue(json_input, output_dir, file_name)
        process_key_value.process_input()
        process_key_value.DataFrame_analytic()




