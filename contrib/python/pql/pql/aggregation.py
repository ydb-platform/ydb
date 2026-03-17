'''
TODO:

optimize adds, multiplies, 'or' and 'and' as they can accept more than two values
validate type info on specific functions
'''
from .matching import AstHandler, ParseError, DateTimeFunc

class AggregationParser(AstHandler):

    FUNC_TO_ARGS = {'concat': '+', # more than 1
                    'strcasecmp': 2,
                    'substr': 3,
                    'toLower': 1,
                    'toUpper': 1,

                    'dayOfYear': 1,
                    'dayOfMonth': 1,
                    'dayOfWeek': 1,
                    'year': 1,
                    'month': 1,
                    'week': 1,
                    'hour': 1,
                    'minute': 1,
                    'second': 1,
                    'millisecond': 1,
                    
                    'date': 1,

                    'cmp': 2,

                    'ifnull': 2}

    SPECIAL_VALUES = {'False': False,
                      'false': False,
                      'True': True,
                      'true': True,
                      'None': None,
                      'null': None}
    
    def handle_Str(self, node):
        return node.s

    def handle_Num(self, node):
        return node.n

    def handle_Name(self, node):
        return self.SPECIAL_VALUES.get(node.id, '$' + node.id)

    def handle_Attribute(self, node):
        return '${0}.{1}'.format(self.handle(node.value), node.attr).replace('$$', '$')

    def handle_UnaryOp(self, op):
        return {self.handle(op.op): self.handle(op.operand)}

    def handle_IfExp(self, op):
        return {'$cond': [self.handle(op.test),
                          self.handle(op.body),
                          self.handle(op.orelse)]}

    def handle_Call(self, node):
        name = node.func.id
        if name == 'date':
            return DateTimeFunc().handle_date(node)
        if name not in self.FUNC_TO_ARGS:
            raise ParseError('Unsupported function ({0}).'.format(name),
                             col_offset=node.col_offset)
        if len(node.args) != self.FUNC_TO_ARGS[name] and \
           self.FUNC_TO_ARGS[name] != '+' or len(node.args) == 0:
            raise ParseError('Invalid number of arguments to function {0}'.format(name),
                             col_offset=node.col_offset)

        # because of SERVER-9289 the following fails: {'$year': {'$add' :['$time_stamp', 1]}}
        # wrapping both single arg functions in a list solves it: {'$year': [{'$add' :['$time_stamp', 1]}]}
        return {'$' + node.func.id: list(map(self.handle, node.args))}

    def handle_BinOp(self, node):
        return {self.handle(node.op): [self.handle(node.left),
                                       self.handle(node.right)]}

    def handle_Not(self, not_node):
        return '$not'

    def handle_And(self, op):
        return '$and'

    def handle_Or(self, op):
        return '$or'

    def handle_BoolOp(self, op):
        return {self.handle(op.op): list(map(self.handle, op.values))}

    def handle_Compare(self, node):
        if len(node.ops) != 1:
            raise ParseError('Invalid number of comparators: {0}'.format(len(node.ops)),
                             col_offset=node.comparators[1].col_offset)
        return {self.handle(node.ops[0]): [self.handle(node.left),
                                           self.handle(node.comparators[0])]}

    def handle_Gt(self, node):
        return '$gt'
        
    def handle_Lt(self,node):
        return '$lt'
        
    def handle_GtE(self, node):
        return '$gte'
        
    def handle_LtE(self, node):
        return '$lte'

    def handle_Eq(self, node):
        return '$eq'
        
    def handle_NotEq(self, node):
        return '$ne'

    def handle_Add(self, node):
        return '$add'

    def handle_Sub(self, node):
        return '$subtract'

    def handle_Mod(self, node):
        return '$mod'

    def handle_Mult(self, node):
        return '$multiply'

    def handle_Div(self, node):
        return '$divide'

class AggregationGroupParser(AstHandler):
    GROUP_FUNCTIONS = ['addToSet', 'push', 'first', 'last',
                       'max', 'min', 'avg', 'sum']
    def handle_Call(self, node):
        if len(node.args) != 1:
            raise ParseError('The {0} group aggregation function accepts one argument'.format(node.func.id),
                             col_offset=node.col_offset)
        if node.func.id not in self.GROUP_FUNCTIONS:
            raise ParseError('Unsupported group function: {0}'.format(node.func.id),
                             col_offset=node.col_offset,
                             options=self.GROUP_FUNCTIONS)
        return {'$' + node.func.id: AggregationParser().handle(node.args[0])}

