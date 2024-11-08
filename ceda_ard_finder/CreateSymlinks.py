import luigi
import json
import os
import logging

from luigi.util import requires
from .SearchForProducts import SearchForProducts
from .SearchTextFileList import SearchTextFileList

log = logging.getLogger("luigi-interface")


class CreateSymlinks(luigi.Task):
    stateFolder = luigi.Parameter()
    productLocation = luigi.Parameter()

    def run(self):
        products = []
        with self.input().open('r') as searchForProductsFile:
            products = json.load(searchForProductsFile)['productList']

        for product in products:
            symlinkPath = os.path.join(self.productLocation, os.path.basename(product))
            os.symlink(product, symlinkPath)

        output = {
            "products": products
        }

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateFolder, "CreateSymlinks.json"))


@requires(SearchTextFileList)
class CreateSymlinksFromTextFileList(CreateSymlinks):

    def nullFunction(self):
        pass


@requires(SearchForProducts)
class CreateSymlinksFromFilters(CreateSymlinks):

    def nullFunction(self):
        pass
