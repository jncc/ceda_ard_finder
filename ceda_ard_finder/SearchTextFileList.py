import luigi
import json
import os
import logging
import re

from luigi.util import requires
from .SearchForProducts import SearchForProducts
from .CedaElasticsearchQueryer import CedaElasticsearchQueryer
from datetime import datetime

log = logging.getLogger("luigi-interface")


class SearchTextFileList(luigi.Task):
    """
    Searches for an exact list of files in a text file rather than using filters
    """
    stateFolder = luigi.Parameter()
    textFile = luigi.Parameter()

    def run(self):
        searchTasks = []

        with open(self.textFile, "r") as f:
            products = [x.strip() for x in f.read().splitlines()]

        # searchTask = SearchForProducts(
        #     stateFolder=self.stateFolder,
        #     _stateFileName=f"SearchForProducts.json",
        #     startDate=datetime(2015, 1, 1),
        #     endDate=datetime.today(),
        #     ardFilters=products,
        #     spatialOperator="intersects"
        # )

        # result = yield searchTask

        # with result.open("r") as taskOutput:
        #     pass

        for product in products:
            searchTasks.append(
                SearchForProducts(
                    stateFolder=self.stateFolder,
                    _stateFileName=f"SearchForProducts_{product.replace('.tif', '')}.json",
                    ardFilter=product,
                    startDate=datetime(2015, 1, 1),
                    endDate=datetime.today(),
                    spatialOperator="intersects"
                )
            )

        yield searchTasks

        output = {
            "count": 0,
            "productList": [],
            "missingProducts": []
        }

        for task in searchTasks:
            searchForProducts = {}
            with task.output().open("r") as taskOutput:
                # We are searching for an exact list so each search must only have one result
                searchForProducts = json.load(taskOutput)
                count = searchForProducts["count"]
                productList = searchForProducts["productList"]

                if count == 0:
                    log.warning(f"No results found for filter: {task.ardFilter}")
                    output["missingProducts"].append(task.ardFilter)
                elif count != 1:
                    log.error(f"More than one result found for filter: {task.ardFilter}")
                    raise Exception(f"Must have exactly one result per input product. Be more specific in your search.")

            output["count"] += count
            output["productList"].extend(productList)

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateFolder, "SearchTextFileList.json"))
