'''
A retcode validator

'''


class PepperRetcode(object):
    '''
    Validation container
    '''

    def validate(self, options, result):
        '''
        Validate result dictionary retcode values.

        :param options: optparse options

        :param result: dictionary from Saltstack master

        :return: exit code
        '''
        if options.fail_any:
            return self.validate_fail_any(result)
        if options.fail_any_none:
            return self.validate_fail_any_none(result)
        if options.fail_all:
            return self.validate_fail_all(result)
        if options.fail_all_none:
            return self.validate_fail_all_none(result)
        return 0

    @staticmethod
    def validate_fail_any(result):
        '''
        Validate result dictionary retcode values.
        Returns 0 if no retcode keys.
        Returns first non zero retcode if any of recodes is non zero.

        :param result: dictionary from Saltstack master

        :return: exit code
        '''
        if isinstance(result, list):
            if isinstance(result[0], dict):
                minion = result[0]
                retcodes = list(minion[name].get('retcode')
                                for name in minion if isinstance(minion[name], dict) and
                                minion[name].get('retcode') is not None)
                return next((r for r in retcodes if r != 0), 0)
        return 0

    @staticmethod
    def validate_fail_any_none(result):
        '''
        Validate result dictionary retcode values.
        Returns -1 if no retcode keys.
        Returns first non zero retcode if any of recodes is non zero.

        :param result: dictionary from Saltstack master

        :return: exit code
        '''
        if isinstance(result, list):
            if isinstance(result[0], dict):
                minion = result[0]
                retcodes = list(minion[name].get('retcode')
                                for name in minion if isinstance(minion[name], dict) and
                                minion[name].get('retcode') is not None)
                if not retcodes:
                    return -1  # there are no retcodes
                return next((r for r in retcodes if r != 0), 0)
        return -1

    @staticmethod
    def validate_fail_all(result):
        '''
        Validate result dictionary retcode values.
        Returns 0 if no retcode keys.
        Returns first non zero retcode if all recodes are non zero.

        :param result: dictionary from Saltstack master

        :return: exit code
        '''
        if isinstance(result, list):
            if isinstance(result[0], dict):
                minion = result[0]
                retcodes = list(minion[name].get('retcode')
                                for name in minion if isinstance(minion[name], dict) and
                                minion[name].get('retcode') is not None)
                if all(r != 0 for r in retcodes):
                    return next((r for r in retcodes if r != 0), 0)
        return 0

    @staticmethod
    def validate_fail_all_none(result):
        '''
        Validate result dictionary retcode values.
        Returns -1 if no retcode keys.
        Returns first non zero retcode if all recodes are non zero.

        :param result: dictionary from Saltstack master

        :return: exit code
        '''
        if isinstance(result, list):
            if isinstance(result[0], dict):
                minion = result[0]
                retcodes = list(minion[name].get('retcode')
                                for name in minion if isinstance(minion[name], dict) and
                                minion[name].get('retcode') is not None)
                if not retcodes:
                    return -1  # there are no retcodes
                if all(r != 0 for r in retcodes):
                    return next((r for r in retcodes if r != 0), 0)
                else:
                    return 0
        return -1
