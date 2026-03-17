<%inherit file="html.mako" />

<%block name="header_extra">
<form class="navbar-left inline-form navbar-form" action="${middleware.path}" method="POST">
<div class="btn-group">
% if middleware.on:
    <button type="button" class="btn btn-success active">On</button>
    <button type="submit" class="btn btn-default">Off</button>
</div>
<input type="hidden" name="turn" value="off" />
% else:
    <button type="submit" class="btn btn-default">On</button>
    <button type="button" class="btn btn-danger active">Off</button>
</div>
<input type="hidden" name="turn" value="on" />
% endif
</form>
<form class="navbar-left inline-form navbar-form" action="${middleware.path}" method="POST">
    <button type="submit" class="btn btn-default">Clear</button>
    <input type="hidden" name="clear" value="1" />
</form>
</%block>
