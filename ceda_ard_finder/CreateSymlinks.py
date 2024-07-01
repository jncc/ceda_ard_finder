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
  productLocation = luigi.Parameter()
  s2CloudsBasePath = luigi.Parameter(default="")

  def run(self):
    products = []
    with self.input().open('r') as searchForProductsFile:
      products = json.load(searchForProductsFile)['productList']

    for product in products:
      symlinkPath = os.path.join(self.productLocation, os.path.basename(product))
      if os.path.basename(product).startswith("S2"):
        if self.s2CloudsBasePath != "":
          s2CustomCloud = os.path.join(self.s2CloudsBasePath, re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_clouds.tif", os.path.basename(product)))
          if os.path.exists(s2CustomCloud):
            log.info("Using custom S2 clouds file: %s", s2CustomCloud)
            os.symlink(s2CustomCloud, re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_clouds.tif", symlinkPath))
          else:
            log.info("Custom S2 cloud path not found")
        else:
          os.symlink(re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_clouds.tif", product), re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_clouds.tif", symlinkPath))
        os.symlink(re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_toposhad.tif", product), re.sub(r"_vmsk_sharp_rad_srefdem_stdsref\.tif$" , "_toposhad.tif", symlinkPath))
      os.symlink(product, symlinkPath)
    
    output = {
      "products": products
    }

    with self.output().open("w") as outFile:
      outFile.write(json.dumps(output, indent=4, sort_keys=True))
  
  def output(self):
    return luigi.LocalTarget(os.path.join(self.stateFolder, "CreateSymlinks.json"))



