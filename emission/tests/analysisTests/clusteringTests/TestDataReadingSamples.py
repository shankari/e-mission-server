import emission.core.wrapper.localdate as ecwl
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess

from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import bson.json_util as bju
import emission.storage.timeseries.abstract_timeseries as esta

import emission.tests.common as etc


class TestDataReadingSamples(unittest.TestCase):
    def readTripsFromFile(self, dataFile):
        with open(dataFile) as dect:
            expected_confirmed_trips = json.load(dect, object_hook = bju.object_hook)
        return expected_confirmed_trips

    def readAndStoreTripsFromFile(self, dataFile):
        import emission.core.get_database as edb
        atsdb = edb.get_analysis_timeseries_db()
        etc.createAndFillUUID(self)
        with open(dataFile) as dect:
            expected_confirmed_trips = json.load(dect, object_hook = bju.object_hook)
            for t in expected_confirmed_trips:
                t["user_id"] = self.testUUID
                edb.save(atsdb, t)

    def readAndStoreTripsFromPipeline(self, dataFile):
        etc.setupRealExample(self, dataFile)
        self.entries = json.load(open(dataFile+".user_inputs"), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

    def printTrips(self, confirmed_trips):
        for t in confirmed_trips:
            print(f"{t['metadata']['key']}: {t['data']['start_fmt_time']} -> {t['data']['end_fmt_time']}")

    def clearDBEntries(self):
        import emission.core.get_database as edb
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})

    def testReadDataFromFile(self):
        confirmed_trips = self.readTripsFromFile("emission/tests/data/real_examples/shankari_2016-06-20.expected_confirmed_trips")
        self.printTrips(confirmed_trips)
        # no need to clear here since we didn't put entries into the database

    def testReadAndStoreTripsFromFile(self):
        confirmed_trips = self.readAndStoreTripsFromFile("emission/tests/data/real_examples/shankari_2016-06-20.expected_confirmed_trips")
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        confirmed_trips = list(ts.find_entries(["analysis/confirmed_trip"], None))
        self.printTrips(confirmed_trips)
        self.clearDBEntries()

    def testReadAndStoreTripsFromPipeline(self):
        confirmed_trips = self.readAndStoreTripsFromPipeline("emission/tests/data/real_examples/shankari_2016-06-20")
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        confirmed_trips = list(ts.find_entries(["analysis/confirmed_trip"], None))
        self.printTrips(confirmed_trips)
        self.clearDBEntries()

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()

