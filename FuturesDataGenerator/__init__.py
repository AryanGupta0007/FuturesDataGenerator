from .main import main

class _FuturesDataGeneratorModule:
    def __call__(self, ORB_URL=None, ORB_PASSWORD=None, ORB_USERNAME=None, FUT_CSV_PATH=None):
        if (ORB_PASSWORD == None) or (ORB_USERNAME == None) or (ORB_URL == None):
            return "[ERROR] ORB CREDS or URL not PROVIDED"
        if FUT_CSV_PATH == "NONE":
            return "PROVIDE FUTURES CSV PATH"
        return main(ORB_URL=ORB_URL, ORB_PASSWORD=ORB_PASSWORD, ORB_USERNAME=ORB_USERNAME, FUT_CSV_PATH=FUT_CSV_PATH)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _FuturesDataGeneratorModule()
