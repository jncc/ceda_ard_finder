import luigi
import json
import os
import logging
import re

from luigi.util import requires
from .SearchForProducts import SearchForProducts

log = logging.getLogger("luigi-interface")

@requires(SearchForProducts)
class CreateSymlinks(luigi.Task):
  stateFolder = luigi.Parameter()
  basketFolder = luigi.Parameter()
  ardBasePath = luigi.Parameter(default = "/neodc/sentinel_ard/data/sentinel_2/")
  s2CloudsBasePath = luigi.Parameter()

  def run(self):
    products = []
    with self.input().open('r') as searchForProductsFile:
      products = json.load(searchForProductsFile)['productList']

    for product in products:
      symlinkPath = os.path.join(self.basketFolder, os.path.basename(product))
      if os.path.basename(product).startswith("S2"):
        if re.match(rf"^{self.ardBasePath}*", product): # Check if the product is in the ARD base path since a re.sub won't fail if the pattern is not found
          s2Cloud = os.path.join(re.sub(f"{self.ardBasePath}", f"{self.s2CloudsBasePath}", os.path.dirname(product)), re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_clouds.tif", os.path.basename(product)))
          os.symlink(s2Cloud, re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_clouds.tif", symlinkPath))
        else:
          log.warning(f"Product {product} is not in the ARD base path {self.ardBasePath}. No symlink to the clouds file will be created.")
        os.symlink(re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_toposhad.tif", product), re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_toposhad.tif", symlinkPath))
      os.symlink(product, symlinkPath)
    
    output = {
      "products": products
    }

    with self.output().open("w") as outFile:
      outFile.write(json.dumps(output, indent=4, sort_keys=True))
  
  def output(self):
    return luigi.LocalTarget(os.path.join(self.stateFolder, "CreateSymlinks.json"))



