import luigi
import json
import os
import logging

from luigi.util import requires
from .SearchForProducts import SearchForProducts

log = logging.getLogger("luigi-interface")

@requires(SearchForProducts)
class CreateTextFileList(luigi.Task):
  startDate = luigi.DateParameter()
  endDate = luigi.DateParameter()
  outputLocation = luigi.Parameter()
  descriptor = luigi.Parameter(default="")

  @staticmethod
  def remove_after_osgb(filename):
    head, sep, _ = filename.partition('_osgb')
    return head + sep

  def run(self):
    products = []
    filename = f"{self.startDate:%Y-%m-%d}_{self.endDate:%Y-%m-%d}_{self.descriptor}.txt" 
    with self.input().open('r') as searchForProductsFile:
      products = json.load(searchForProductsFile)['productList']
    products = [p if '/' not in p else p.rsplit('/', 1)[1] for p in products]
    products = [self.remove_after_osgb(p) for p in products]

    with open(os.path.join(self.outputLocation, filename), "w") as outfile:
      for product in products:
        outfile.write(f"{product}\n")
