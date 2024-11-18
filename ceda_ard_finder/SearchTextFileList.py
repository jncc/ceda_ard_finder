import luigi
import json
import os
import logging

from datetime import datetime
from functional import seq

from .SearchForProducts import SearchForProducts

log = logging.getLogger("luigi-interface")


class SearchTextFileList(luigi.Task):
    """
    Searches for an exact list of files in a text file rather than using filters.
    The file should be named inputs.txt and be in the productLocation folder.
    """
    stateFolder = luigi.Parameter()
    productLocation = luigi.Parameter()

    noMissing = luigi.BoolParameter(
        description="If true, the task will fail if any of the products are not found",
        default=False
    )

    sameSatellite = luigi.BoolParameter(
        description="If true, the task will fail if the products are not all from the same satellite",
        default=False
    )

    def run(self):
        searchTasks = []
        products = []

        with open(os.path.join(self.productLocation, "inputs.txt"), "r") as f:
            products = [x.strip() for x in f.read().splitlines()]

        if self.sameSatellite:
            satellites = seq(products) \
                .map(lambda x: x[0:2]) \
                .distinct() \
                .to_list()

            if len(satellites) != 1:
                raise Exception(f"Products must all be from the same satellite. Found: {satellites}")

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
            if len(product) < 10:
                raise Exception(f"Product names from inputs.txt must be at least 10 characters long: {product}")

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

        if self.noMissing:
            if len(output["missingProducts"]) > 0:
                raise Exception(f"Missing products: {output['missingProducts']}")

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateFolder, "SearchTextFileList.json"))
