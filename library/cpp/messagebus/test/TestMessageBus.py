from devtools.fleur.ytest import group, constraint
from devtools.fleur.ytest.integration import UnitTestGroup

@group
@constraint('library.messagebus')
class TestMessageBus(UnitTestGroup): 
    def __init__(self, context):
        UnitTestGroup.__init__(self, context, 'MessageBus', 'library-messagebus-test-ut')
