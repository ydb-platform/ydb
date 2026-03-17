import os
import sys

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # noqa: PTH100, PTH120
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the main function directly from the module
from mcp_graphql.__init__ import main  # noqa: E402

if __name__ == "__main__":
    main()
