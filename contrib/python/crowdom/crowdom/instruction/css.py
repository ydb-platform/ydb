css = """
<style>

    body {
        scroll-behavior: smooth;
    }

    h2, h3, h4 {
        font-weight: inherit;
        font-style: four;
        text-align: left;
        text-transform: none;
        margin: 20px 0 10px 0;
    }

    p {
        font-size: 14px;
        align: left;
        color: #000000;
        margin: 2px 0;
        padding-right: 10px;
    }

    ul, ol {
        padding-left: 15px;
        margin-left: 25px;
    }

    li {
        font-size: 14px;
        align: left;
        color: #000000;
        margin: 2px 0;
        padding-right: 10px;
    }

    .hide {
        display: none
    }

    .hide + label ~ div {
        display: none
    }

    .hide + label ~ p {
        display: none
    }

    .hide + label {
        border-bottom: 1px dotted blue;
        padding: 0;
        margin: 0;
        cursor: pointer;
        display: inline-block;
        color: blue;
        font-size: 14px;
    }

    .hide:checked + label {
        border-bottom: 0;
        color: green;
        padding-right: 10px;
        margin: 0;
        font-size: 14px;
    }

    .hide:checked + label + div {
        display: block;
    }

    .hide:checked + label + span {
        display: block;
    }

    .hide:checked + label + p {
        display: block;
    }

    img {
        width: 100%;
        max-width: 600px;
    }

</style>
"""
