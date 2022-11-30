from tvmauth.tvmauth_pymodule import __remove_ticket_signature


def remove_ticket_signature(ticket_body):
    """
    :param ticket_body: Full ticket body
    :return:            Safe for logging part of ticket - it can be parsed later with `tvmknife parse_ticket ...`
    """
    return __remove_ticket_signature(ticket_body)
