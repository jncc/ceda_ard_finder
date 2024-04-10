import luigi
import json
import os
import logging

from .CedaElasticsearchQueryer import CedaElasticsearchQueryer

#https://elasticsearch.ceda.ac.uk/ceda-eo/_search
#"POLYGON((-3.8295000316687355 53.23354636293673, -2.7418535472937355 53.23354636293673, -2.7418535472937355 52.484002471274685, -3.8295000316687355 52.484002471274685, -3.8295000316687355 53.23354636293673))"

log = logging.getLogger("luigi-interface")

class SearchForProducts(luigi.Task):
  stateLocation = luigi.Parameter()
  ardFilter = luigi.Parameter(default="")

  # search filters
  startDate = luigi.DateParameter()
  endDate = luigi.DateParameter()
  wkt = luigi.Parameter(default="")
  spatialOperator = luigi.ChoiceParameter(choices=["", "intersects", "disjoint", "contains", "within"])
  satelliteFilter = luigi.Parameter(default="")
  orbit = luigi.IntParameter(default="")
  orbitDirection = luigi.Parameter(default="")

  elasticsearchHost = luigi.Parameter(default="elasticsearch.ceda.ac.uk")
  elasticsearchPort = luigi.IntParameter(default=443)
  elasticsearchIndex = luigi.Parameter(default="ceda-eo")
  elasticsearchPageSize = luigi.IntParameter(default=100)

  def parseResults(self, results):
    productList = []
    for result in results['hits']['hits']:
      dataFilepath = os.path.join(result['_source']['file']['directory'], result['_source']['file']['data_file'])
      productList.append(dataFilepath)

    return productList

  def queryAllResults(self, queryer):
    productList = []
    results = queryer.query(index=self.elasticsearchIndex, start=0, size=self.elasticsearchPageSize)
    log.info(results)
    productList.extend(self.parseResults(results))

    while len(productList) < results['hits']['total']['value']:
      nextPage = queryer.query(index=self.elasticsearchIndex, start=len(productList), size=self.elasticsearchPageSize)
      log.info(nextPage)
      productList.extend(self.parseResults(nextPage))

    return productList

  def run(self):
    queryer = CedaElasticsearchQueryer(host=self.elasticsearchHost, port=self.elasticsearchPort)

    if self.spatialOperator != "" and self.wkt != "":
      queryer.addGeoFilter(spatialOperation=self.spatialOperator, wkt=self.wkt)
    
    if self.startDate != None and self.endDate != None:
      queryer.addDateFilter(start_date=self.startDate, end_date=self.endDate)
    
    satelliteFilterArray = []

    if self.satelliteFilter != "":
      satelliteFilterArray = [x.strip() for x in self.satelliteFilter.split(',')]
      queryer.addSatelliteFilters(satelliteFilterArray)

    if self.orbit != "":
      queryer.addOrbitFilter(self.orbit)

    if self.orbitDirection != "":
      queryer.addOrbitDirectionFilter(self.orbitDirection)

    if self.ardFilter != "":
      queryer.addArdFilter(self.ardFilter)

      if self.satelliteFilter == "":
        queryer.addSatelliteFilters([f"Sentinel-{self.ardFilter.strip()[1:3]} ARD"])

    productList = self.queryAllResults(queryer)
    
    output = {
      "count": len(productList),
      "productList": productList
    }

    with self.output().open("w") as outFile:
      outFile.write(json.dumps(output, indent=4, sort_keys=True))
  
  def output(self):
    return luigi.LocalTarget(os.path.join(self.stateLocation, "SearchForProducts.json"))



